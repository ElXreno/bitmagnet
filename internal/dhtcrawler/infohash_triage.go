package dhtcrawler

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// runInfoHashTriage receives discovered hashes on the infoHashTriage channel, determines if they should be crawled,
// and forwards them to the appropriate channel. Possible outcomes are:
//  1. The hash is not in the database, so it is forwarded to the getPeers channel to attempt
//     retrieval of the meta info.
//  2. The hash is in the database, but we don't have the full details of the torrent (for example it was imported
//     outside the DHT crawler, and so we don't have the files info), so it is forwarded to the getPeers channel to
//     attempt retrieval of the meta info.
//  3. The hash is in the database, but the seeders/leechers are not known or are outdated,
//     so it is forwarded to the scrape channel.
//  4. The hash is in the database and the seeders/leechers are known and up to date, so it is discarded.
func (c *crawler) runInfoHashTriage(
	ctx context.Context,
	name string,
	in concurrency.BatchingChannel[nodeHasPeersForHash],
) {
	for {
		select {
		case <-ctx.Done():
			return
		case reqs := <-in.Out():
			started := time.Now()

			c.countHash(name, "batches", 1)
			c.countHash(name, "batch_hashes", len(reqs))
			allHashes := make([]protocol.ID, 0, len(reqs))

			reqMap := make(map[protocol.ID]nodeHasPeersForHash, len(reqs))
			for _, r := range reqs {
				if _, ok := reqMap[r.infoHash]; ok {
					continue
				}

				allHashes = append(allHashes, r.infoHash)
				reqMap[r.infoHash] = r
			}

			filteredHashes, filterErr := c.blockingManager.Filter(ctx, allHashes)
			c.countHash(name, "filter_ms", int(time.Since(started).Milliseconds()))
			if filterErr != nil {
				c.logger.Errorf("failed to filter infohashes: %s", filterErr.Error())
				break
			}

			if len(filteredHashes) == 0 {
				break
			}

			filteredHashMap := make(map[protocol.ID]struct{}, len(filteredHashes))
			valuers := make([]driver.Valuer, 0, len(filteredHashes))

			for _, h := range filteredHashes {
				filteredHashMap[h] = struct{}{}

				valuers = append(valuers, h)
			}

			for h, r := range reqMap {
				if _, ok := filteredHashMap[h]; !ok {
					c.countHash(r.source, stageBlocked, 1)
				}
			}

			var result []*triageResult
			if queryErr := c.dao.Torrent.WithContext(ctx).Select(
				c.dao.Torrent.InfoHash,
				c.dao.Torrent.FilesStatus,
				c.dao.Torrent.FilesCount,
				c.dao.TorrentsTorrentSource.Seeders,
				c.dao.TorrentsTorrentSource.Leechers,
				c.dao.TorrentsTorrentSource.UpdatedAt,
			).LeftJoin(
				c.dao.TorrentsTorrentSource,
				c.dao.Torrent.InfoHash.EqCol(c.dao.TorrentsTorrentSource.InfoHash),
				c.dao.TorrentsTorrentSource.Source.Eq("dht"),
			).Where(
				c.dao.Torrent.InfoHash.In(valuers...),
			).UnderlyingDB().Find(&result).Error; queryErr != nil {
				c.logger.Errorf("failed to search existing torrents: %s", queryErr.Error())
				break
			}

			c.countHash(name, "db_ms", int(time.Since(started).Milliseconds()))

			sendStarted := time.Now()
			foundTorrents := make(map[protocol.ID]triageResult)
			for _, t := range result {
				foundTorrents[t.InfoHash] = *t
			}

			for h := range filteredHashMap {
				r := reqMap[h]
				if t, ok := foundTorrents[r.infoHash]; !ok ||
					t.FilesStatus == model.FilesStatusNoInfo ||
					(t.FilesStatus != model.FilesStatusSingle && !t.FilesCount.Valid) ||
					(t.FilesStatus == model.FilesStatusOverThreshold && t.FilesCount.Uint <= c.saveFilesThreshold) {
					c.countHash(r.source, stageNeedsMetaInfo, 1)
					c.trackers.submit(r.infoHash)

					sendOne := time.Now()

					select {
					case <-ctx.Done():
						return
					case c.getPeers.In() <- r:
						c.countHash(name, "send_get_peers_us", int(time.Since(sendOne).Microseconds()))

						continue
					}
				} else if r.source == sourceSample && ((!t.Seeders.Valid || !t.Leechers.Valid) ||
					t.UpdatedAt.Before(time.Now().Add(-c.rescrapeThreshold))) {
					c.countHash(r.source, stageScrape, 1)

					sendOne := time.Now()

					select {
					case <-ctx.Done():
						return
					case c.scrape.In() <- r:
						c.countHash(name, "send_scrape_us", int(time.Since(sendOne).Microseconds()))

						continue
					default:
						c.countHash(r.source, stageScrapeDropped, 1)

						continue
					}
				} else {
					c.countHash(r.source, stageKnown, 1)
				}
			}

			c.countHash(name, "send_ms", int(time.Since(sendStarted).Milliseconds()))
		}
	}
}

type triageResult struct {
	InfoHash    protocol.ID
	FilesStatus model.FilesStatus
	FilesCount  model.NullUint
	Seeders     model.NullUint
	Leechers    model.NullUint
	UpdatedAt   time.Time
}
