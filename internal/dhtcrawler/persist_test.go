package dhtcrawler

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/bloom"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type lenientConverter struct{}

func (lenientConverter) ConvertValue(v any) (driver.Value, error) {
	if valuer, ok := v.(driver.Valuer); ok {
		inner, err := valuer.Value()
		if err != nil {
			return nil, err
		}

		v = inner
	}

	return driver.DefaultParameterConverter.ConvertValue(v)
}

func newPersistTestCrawler(t *testing.T) (*crawler, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.ValueConverterOption(lenientConverter{}))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	c := newTestCrawler(nil, nil)
	c.dao = dao.Use(gormDB)
	c.logger = zap.NewNop().Sugar()
	c.persistTorrents = concurrency.NewBatchingChannel[infoHashWithMetaInfo](10, 100, time.Hour)
	c.persistSources = concurrency.NewBatchingChannel[infoHashWithScrape](10, 100, time.Hour)

	return c, mock
}

func stoppedContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}

func TestRunPersistSourcesDrainsOnStop(t *testing.T) {
	t.Parallel()

	c, mock := newPersistTestCrawler(t)
	mock.ExpectBegin().WillReturnError(errors.New("database is down"))

	filter := *bloom.NewWithEstimates(10, 0.01)
	c.persistSources.In() <- infoHashWithScrape{
		nodeHasPeersForHash: nodeHasPeersForHash{infoHash: protocol.ID{1}},
		bfsd:                filter,
		bfpe:                filter,
	}

	c.runPersistSources(stoppedContext())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the pending scrape batch was not persisted on stop: %v", err)
	}
}

func TestRunPersistTorrentsDrainsOnStop(t *testing.T) {
	t.Parallel()

	c, mock := newPersistTestCrawler(t)
	mock.ExpectBegin().WillReturnError(errors.New("database is down"))

	c.persistTorrents.In() <- infoHashWithMetaInfo{
		nodeHasPeersForHash: nodeHasPeersForHash{infoHash: protocol.ID{2}, source: sourceSample},
		metaInfo:            metainfo.Info{Name: "pending", PieceLength: 16384},
	}

	c.runPersistTorrents(stoppedContext())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the pending torrent batch was not persisted on stop: %v", err)
	}
}

func TestRunPersistStopsWithNothingPending(t *testing.T) {
	t.Parallel()

	c, mock := newPersistTestCrawler(t)

	done := make(chan struct{})

	go func() {
		c.runPersistTorrents(stoppedContext())
		c.runPersistSources(stoppedContext())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the persist workers did not stop")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunPersistSourcesFinishesTheBatchInFlightOnStop(t *testing.T) {
	t.Parallel()

	c, mock := newPersistTestCrawler(t)
	c.persistSources = concurrency.NewBatchingChannel[infoHashWithScrape](10, 1, time.Hour)

	mock.ExpectBegin().WillDelayFor(300 * time.Millisecond)
	mock.ExpectExec(".*").WillReturnError(errors.New("database is down"))
	mock.ExpectRollback()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := *bloom.NewWithEstimates(10, 0.01)
	c.persistSources.In() <- infoHashWithScrape{
		nodeHasPeersForHash: nodeHasPeersForHash{infoHash: protocol.ID{3}},
		bfsd:                filter,
		bfpe:                filter,
	}

	done := make(chan struct{})

	go func() {
		c.runPersistSources(ctx)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the persist worker did not stop")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the batch in flight was cancelled by the stop: %v", err)
	}
}

func TestGraceContextOutlivesItsParentByTheGrace(t *testing.T) {
	t.Parallel()

	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := graceContext(parent, 200*time.Millisecond)

	defer cancel()

	stop()

	select {
	case <-ctx.Done():
		t.Fatal("the context ended with its parent")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the context did not end after the grace")
	}
}
