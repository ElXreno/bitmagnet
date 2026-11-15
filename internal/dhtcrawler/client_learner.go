package dhtcrawler

import (
	"encoding/json"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
)

const (
	clientLearnerFile    = "closed_clients.json"
	clientLearnerMaxKeys = 4000
	// a closed client may fetch this share of its swarms before it is no longer one
	clientLearnerForgive  = 0.01
	clientLearnerProbeMin = 100
)

type clientStat struct {
	Swarms  int  `json:"swarms"`
	Fetched int  `json:"fetched"`
	Closed  bool `json:"closed"`

	Probed       int `json:"probed,omitempty"`
	ProbeFetched int `json:"probe_fetched,omitempty"`
}

// clientLearner finds closed networks by what they do: a client whose swarms never give meta info to
// anyone is one, whatever it calls itself. A client that merely holds none of its own is not.
type clientLearner struct {
	mu        sync.Mutex
	path      string
	minSwarms int
	maxShare  float64
	stats     map[string]*clientStat
	onChange  func(key string, stat clientStat)
	probe     func() bool
}

func probePercent() int {
	if v, err := strconv.Atoi(os.Getenv("BITMAGNET_CLOSED_LEARN_PROBE_PERCENT")); err == nil && v >= 0 {
		return min(v, 100)
	}

	return 2
}

func newClientLearner(path string, minSwarms int, maxShare float64) *clientLearner {
	percent := probePercent()
	l := &clientLearner{
		path: path, minSwarms: minSwarms, maxShare: maxShare, stats: make(map[string]*clientStat),
		probe: func() bool { return rand.IntN(100) < percent },
	}

	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &l.stats)
	}

	for key, stat := range l.stats {
		if stat == nil {
			delete(l.stats, key)
		}
	}

	return l
}

func (l *clientLearner) isClosed(key string) bool {
	if l == nil {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	stat := l.stats[key]

	return stat != nil && stat.Closed
}

func (l *clientLearner) shouldProbe() bool {
	return l != nil && l.probe != nil && l.probe()
}

// observe records one swarm in which peers of these clients completed a handshake.
func (l *clientLearner) observe(keys map[string]struct{}, fetched bool) {
	if l == nil || len(keys) == 0 {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for key := range keys {
		stat := l.stats[key]
		if stat != nil && stat.Closed {
			continue
		}

		if stat == nil {
			if len(l.stats) >= clientLearnerMaxKeys {
				l.dropRarest()
			}

			stat = &clientStat{}
			l.stats[key] = stat
		}

		stat.Swarms++

		if fetched {
			stat.Fetched++
		}

		if stat.Swarms >= l.minSwarms && float64(stat.Fetched)/float64(stat.Swarms) <= l.maxShare {
			stat.Closed = true

			if l.onChange != nil {
				l.onChange(key, *stat)
			}
		}
	}
}

func (l *clientLearner) observeProbe(keys map[string]struct{}, fetched bool) int {
	if l == nil || len(keys) == 0 {
		return 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	forgiven := 0

	for key := range keys {
		stat := l.stats[key]
		if stat == nil || !stat.Closed {
			continue
		}

		stat.Probed++

		if fetched {
			stat.ProbeFetched++
		}

		if stat.Probed < clientLearnerProbeMin || float64(stat.ProbeFetched)/float64(stat.Probed) <= clientLearnerForgive {
			continue
		}

		stat.Closed = false
		forgiven++

		if l.onChange != nil {
			l.onChange(key, *stat)
		}

		delete(l.stats, key)
	}

	return forgiven
}

func (l *clientLearner) dropRarest() {
	keys := make([]string, 0, len(l.stats))
	for key, stat := range l.stats {
		if !stat.Closed {
			keys = append(keys, key)
		}
	}

	sort.Slice(keys, func(i, j int) bool { return l.stats[keys[i]].Swarms < l.stats[keys[j]].Swarms })

	for _, key := range keys[:min(len(keys), clientLearnerMaxKeys/4)] {
		delete(l.stats, key)
	}
}

func (l *clientLearner) closed() map[string]clientStat {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	out := make(map[string]clientStat)

	for key, stat := range l.stats {
		if stat.Closed {
			out[key] = *stat
		}
	}

	return out
}

func (l *clientLearner) save() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	data, err := json.Marshal(l.stats)
	l.mu.Unlock()

	if err != nil {
		return err
	}

	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}

	return os.Rename(tmp, l.path)
}
