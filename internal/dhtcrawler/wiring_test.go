package dhtcrawler

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func TestBothRoutingTablesAreWired(t *testing.T) {
	t.Parallel()

	type workers struct {
		fx.In
		Workers []worker.Worker `group:"workers"`
	}

	err := fx.ValidateApp(
		fx.Provide(
			fx.Annotated{Name: "dht_node_id", Target: protocol.RandomNodeID},
			fx.Annotated{
				Name:   "metainfo_banning_checker",
				Target: func() banning.Checker { return allowAll{} },
			},
			client.New,
			ktable.New,
			responder.New,
			server.New,
			server.NewDefaultConfig,
			New,
			NewDiscoveredNodes,
			NewDefaultConfig,
			func() *zap.SugaredLogger { return zap.NewNop().Sugar() },
			func() metainforequester.Requester { return fakeRequester{} },
			func() lazy.Lazy[search.Search] { return nil },
			func() lazy.Lazy[*dao.Query] { return nil },
			func() lazy.Lazy[blocking.Manager] { return nil },
		),
		fx.Invoke(func(workers) {}),
	)
	if err != nil {
		t.Fatal(err)
	}
}
