package dhtcrawler

import (
	"context"
	"errors"
	"net/netip"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
)

func keys(names ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		out[name] = struct{}{}
	}

	return out
}

func TestClientLearnerJudgesBySwarms(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), clientLearnerFile)
	l := newClientLearner(path, 100, 0.002)

	var changes []string

	l.onChange = func(key string, stat clientStat) {
		if stat.Closed {
			changes = append(changes, "+"+key)
		} else {
			changes = append(changes, "-"+key)
		}
	}

	for i := range 99 {
		l.observe(keys("-XT1000-"), false)
		l.observe(keys("-qB5300-"), i%3 == 0)
		l.observe(keys("-MP0001-", "-qB5300-"), true)
	}

	if l.isClosed("-XT1000-") {
		t.Fatal("99 swarms are not enough to judge")
	}

	l.observe(keys("-XT1000-"), false)

	if !l.isClosed("-XT1000-") || l.isClosed("-qB5300-") {
		t.Fatal("a client whose 100 swarms gave nothing is closed, one that delivers is not")
	}

	if l.isClosed("-MP0001-") {
		t.Fatal("a client that holds no meta info but sits in swarms that deliver is not closed")
	}

	if err := l.save(); err != nil {
		t.Fatal(err)
	}

	again := newClientLearner(path, 100, 0.002)
	if !again.isClosed("-XT1000-") || again.isClosed("-qB5300-") {
		t.Fatal("the verdicts must survive a restart")
	}

	for range 2 {
		l.observe(keys("-XT1000-"), true)
	}

	if l.isClosed("-XT1000-") {
		t.Fatal("a closed client whose swarms start to deliver is forgiven")
	}

	if len(changes) != 2 || changes[0] != "+-XT1000-" || changes[1] != "--XT1000-" {
		t.Fatalf("changes %v", changes)
	}

	var none *clientLearner

	none.observe(keys("x"), true)

	if none.isClosed("x") || none.save() != nil || none.closed() != nil {
		t.Fatal("a nil learner does nothing")
	}
}

func TestClientLearnerKeepsItsSizeBounded(t *testing.T) {
	t.Parallel()

	l := newClientLearner(filepath.Join(t.TempDir(), clientLearnerFile), 2, 0)

	l.observe(keys("closed"), false)
	l.observe(keys("closed"), false)

	for i := range clientLearnerMaxKeys * 2 {
		l.observe(keys(string(rune('a'+i%26))+string(rune('0'+i/26%10))+string(rune(0x100+i))), true)
	}

	if len(l.stats) > clientLearnerMaxKeys || !l.isClosed("closed") {
		t.Fatalf("%d keys, closed verdict kept: %v", len(l.stats), l.isClosed("closed"))
	}
}

type silentClientRequester struct {
	calls *atomic.Int32
}

func (f silentClientRequester) Request(
	context.Context,
	protocol.ID,
	netip.AddrPort,
) (metainforequester.Response, error) {
	f.calls.Add(1)

	return metainforequester.Response{}, &metainforequester.PeerError{
		PeerID: protocol.ID([]byte("-ZZ9999-abcdefghijkl")),
		Err:    errors.New("no extended handshake"),
	}
}

func TestLearnedClosedClientEndsTheSwarm(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(12)
	calls := new(atomic.Int32)

	c := newTestCrawler(alive, nil)
	c.closedClients = newClientLearner(filepath.Join(t.TempDir(), clientLearnerFile), 3, 0)
	c.metainfoRequester = silentClientRequester{calls: calls}

	for i := range 3 {
		_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{byte(i + 1)}, peers[:1])
		if err == nil || errors.Is(err, errClosedSwarm) {
			t.Fatalf("swarm %d: an unknown client is an ordinary failure until it is learned, got %v", i, err)
		}
	}

	if !c.closedClients.isClosed("-ZZ9999-") {
		t.Fatal("three swarms without meta info teach the client")
	}

	calls.Store(0)

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{9}, peers)
	if !errors.Is(err, errClosedSwarm) {
		t.Fatalf("want errClosedSwarm from a learned client, got %v", err)
	}

	if got := calls.Load(); got != metaInfoParallelDials {
		t.Fatalf("want only the first %d dials, got %d", metaInfoParallelDials, got)
	}
}
