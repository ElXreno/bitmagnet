package dhtcrawler

import (
	"context"
	"errors"
	"net/netip"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
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

	for range 100 {
		l.observe(keys("-XT1000-"), true)
	}

	if !l.isClosed("-XT1000-") {
		t.Fatal("swarms that did not probe the closed client cannot forgive it")
	}

	for i := range clientLearnerProbeMin {
		if l.isClosed("-XT1000-") != (i < clientLearnerProbeMin) {
			t.Fatalf("probe %d: forgiven too early", i)
		}

		if got := l.observeProbe(keys("-XT1000-"), i < 2); (got == 1) != (i == clientLearnerProbeMin-1) {
			t.Fatalf("probe %d: %d forgiven", i, got)
		}
	}

	if l.isClosed("-XT1000-") {
		t.Fatal("a closed client whose probed swarms start to deliver is forgiven")
	}

	if stat := l.stats["-XT1000-"]; stat != nil {
		t.Fatalf("a forgiven client starts from zero, has %+v", *stat)
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

func TestClientLearnerProbesJudgeClosedClients(t *testing.T) {
	t.Parallel()

	l := newClientLearner(filepath.Join(t.TempDir(), clientLearnerFile), 2, 0)

	l.observe(keys("-XT1000-"), false)
	l.observe(keys("-XT1000-"), false)

	if !l.isClosed("-XT1000-") {
		t.Fatal("two swarms without meta info close the client")
	}

	if got := l.observeProbe(keys("-XT1000-", "-qB5300-"), true); got != 0 {
		t.Fatalf("%d forgiven", got)
	}

	if _, ok := l.stats["-qB5300-"]; ok {
		t.Fatal("a probe only counts for closed clients")
	}

	for range clientLearnerProbeMin * 2 {
		l.observeProbe(keys("-XT1000-"), false)
	}

	if !l.isClosed("-XT1000-") {
		t.Fatal("a closed client whose probes give nothing stays closed")
	}

	stat := l.closed()["-XT1000-"]
	if stat.Swarms != 2 || stat.Probed != 1+clientLearnerProbeMin*2 || stat.ProbeFetched != 1 {
		t.Fatalf("stat %+v", stat)
	}
}

func TestClientLearnerForgivenStartsLearningAgain(t *testing.T) {
	t.Parallel()

	l := newClientLearner(filepath.Join(t.TempDir(), clientLearnerFile), 3, 0)

	for range 3 {
		l.observe(keys("-XT1000-"), false)
	}

	for range clientLearnerProbeMin {
		l.observeProbe(keys("-XT1000-"), true)
	}

	if l.isClosed("-XT1000-") {
		t.Fatal("forgiven")
	}

	l.observe(keys("-XT1000-"), false)
	l.observe(keys("-XT1000-"), false)

	if l.isClosed("-XT1000-") {
		t.Fatal("two swarms are not enough to judge it again")
	}

	l.observe(keys("-XT1000-"), false)

	if !l.isClosed("-XT1000-") {
		t.Fatal("three fresh swarms without meta info close it again")
	}
}

func TestProbePercentFromEnv(t *testing.T) {
	for value, want := range map[string]int{"": 2, "junk": 2, "-1": 2, "0": 0, "7": 7, "250": 100} {
		t.Setenv("BITMAGNET_CLOSED_LEARN_PROBE_PERCENT", value)

		if got := probePercent(); got != want {
			t.Fatalf("%q: want %d, got %d", value, want, got)
		}
	}
}

type partlySilentRequester struct {
	good   map[netip.AddrPort]bool
	goodID protocol.ID
	static map[netip.AddrPort]bool
	calls  *atomic.Int32
	silent *atomic.Int32
}

func (f partlySilentRequester) Request(
	ctx context.Context,
	hash protocol.ID,
	addr netip.AddrPort,
) (metainforequester.Response, error) {
	f.calls.Add(1)

	if f.good[addr] {
		for f.silent.Load() == 0 {
			time.Sleep(time.Millisecond)
		}

		time.Sleep(20 * time.Millisecond)

		res := metainforequester.Response{Info: metainfo.Info{PieceLength: 16384}}
		res.PeerID = f.goodID

		return res, nil
	}

	if f.static[addr] {
		return metainforequester.Response{}, &metainforequester.PeerError{
			PeerID: protocol.ID([]byte("-XT1000-abcdefghijkl")),
			Err:    errors.Join(metainforequester.ErrClosedNetworkPeer, errors.New("no extended handshake")),
		}
	}

	defer f.silent.Add(1)

	return silentClientRequester{calls: new(atomic.Int32)}.Request(ctx, hash, addr)
}

func closedClientCrawler(
	t *testing.T,
	probe bool,
	good, static, alive map[netip.AddrPort]bool,
	goodID protocol.ID,
	calls *atomic.Int32,
) *crawler {
	t.Helper()

	c := newTestCrawler(alive, nil)
	c.closedClients = newClientLearner(filepath.Join(t.TempDir(), clientLearnerFile), 1, 0)
	c.closedClients.probe = func() bool { return probe }
	c.metainfoRequester = partlySilentRequester{
		good: good, goodID: goodID, static: static, calls: calls, silent: new(atomic.Int32),
	}
	c.closedClients.observe(keys("-ZZ9999-"), false)

	if !c.closedClients.isClosed("-ZZ9999-") {
		t.Fatal("not closed")
	}

	return c
}

func TestProbedSwarmIgnoresLearnedClosedClient(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(12)
	calls := new(atomic.Int32)
	good := map[netip.AddrPort]bool{peers[9]: true}
	c := closedClientCrawler(t, true, good, nil, alive, protocol.ID{}, calls)

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
	if err != nil {
		t.Fatalf("a probed swarm goes on past the closed client, got %v", err)
	}

	for stage, want := range map[string]float64{
		"closed_client_probe": 1, "closed_client_probe_fetched": 0, "closed_client_learned": 0, "closed_client_forgiven": 0,
	} {
		if got := counted(t, c.hashesTotal, sourceSample, stage); got != want {
			t.Fatalf("%s: want %v, got %v", stage, want, got)
		}
	}

	if stat := c.closedClients.closed()["-ZZ9999-"]; stat.Probed != 1 || stat.ProbeFetched != 0 {
		t.Fatalf("a fetch by another client is no proof for the closed one: stat %+v", stat)
	}
}

func TestProbedSwarmWithoutMetaInfoIsAnOrdinaryFailure(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(12)
	calls := new(atomic.Int32)
	c := closedClientCrawler(t, true, nil, nil, alive, protocol.ID{}, calls)

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
	if err == nil || errors.Is(err, errClosedSwarm) {
		t.Fatalf("a probed swarm is not condemned as closed, got %v", err)
	}

	if got := calls.Load(); got != 12 {
		t.Fatalf("want all 12 peers dialed, got %d", got)
	}

	if got := counted(t, c.hashesTotal, sourceSample, "closed_client_probe"); got != 1 {
		t.Fatalf("one probed swarm, got %v", got)
	}

	if got := counted(t, c.hashesTotal, sourceSample, "closed_client_probe_fetched"); got != 0 {
		t.Fatalf("nothing fetched, got %v", got)
	}

	if stat := c.closedClients.closed()["-ZZ9999-"]; stat.Probed != 1 || stat.ProbeFetched != 0 {
		t.Fatalf("stat %+v", stat)
	}
}

func TestUnprobedSwarmStillStopsAtLearnedClosedClient(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(12)
	calls := new(atomic.Int32)
	c := closedClientCrawler(t, false, nil, nil, alive, protocol.ID{}, calls)

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
	if !errors.Is(err, errClosedSwarm) {
		t.Fatalf("want errClosedSwarm, got %v", err)
	}

	if got := calls.Load(); got != metaInfoParallelDials {
		t.Fatalf("want only the first %d dials, got %d", metaInfoParallelDials, got)
	}

	if got := counted(t, c.hashesTotal, sourceSample, "closed_client_probe"); got != 0 {
		t.Fatalf("no probe, got %v", got)
	}

	if stat := c.closedClients.closed()["-ZZ9999-"]; stat.Probed != 0 {
		t.Fatalf("stat %+v", stat)
	}
}

func TestProbeForgivesClosedClientThroughTheCrawler(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(metaInfoParallelDials + 1)
	calls := new(atomic.Int32)
	good := map[netip.AddrPort]bool{peers[metaInfoParallelDials]: true}
	c := closedClientCrawler(t, true, good, nil, alive, protocol.ID([]byte("-ZZ9999-mnopqrstuvwx")), calls)

	for i := range clientLearnerProbeMin {
		if _, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{byte(i + 1)}, peers); err != nil {
			t.Fatal(err)
		}
	}

	if c.closedClients.isClosed("-ZZ9999-") {
		t.Fatal("forgiven after the probes delivered")
	}

	if got := counted(t, c.hashesTotal, sourceSample, "closed_client_forgiven"); got != 1 {
		t.Fatalf("want one forgiven, got %v", got)
	}
}

func TestClosedClientThatFetchesIsCreditedInAProbedSwarm(t *testing.T) {
	t.Parallel()

	for name, probe := range map[string]bool{"probed": true, "not probed": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			peers, alive := testPeers(metaInfoParallelDials)
			calls := new(atomic.Int32)
			good := map[netip.AddrPort]bool{peers[0]: true}
			c := closedClientCrawler(t, probe, good, nil, alive, protocol.ID([]byte("-ZZ9999-mnopqrstuvwx")), calls)

			if _, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers); err != nil {
				t.Fatalf("the closed client's peer served the swarm, got %v", err)
			}

			want := 0
			if probe {
				want = 1
			}

			if got := counted(t, c.hashesTotal, sourceSample, "closed_client_probe_fetched"); got != float64(want) {
				t.Fatalf("want %d probe fetches, got %v", want, got)
			}

			if stat := c.closedClients.closed()["-ZZ9999-"]; stat.Probed != want || stat.ProbeFetched != want {
				t.Fatalf("stat %+v", stat)
			}
		})
	}
}

func TestSwarmCutByAnotherClosedPeerIsNoProbe(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(metaInfoParallelDials)
	calls := new(atomic.Int32)
	static := map[netip.AddrPort]bool{peers[0]: true}
	c := closedClientCrawler(t, true, nil, static, alive, protocol.ID{}, calls)

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
	if !errors.Is(err, errClosedSwarm) {
		t.Fatalf("want errClosedSwarm, got %v", err)
	}

	if stat := c.closedClients.closed()["-ZZ9999-"]; stat.Probed != 0 {
		t.Fatalf("a swarm cut by another closed peer gave no chance to fetch: stat %+v", stat)
	}
}
