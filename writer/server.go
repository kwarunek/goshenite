package main

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/panjf2000/gnet/v2"
)

type GosheniteServer struct {
	gnet.BuiltinEventEngine

	eng       gnet.Engine
	addr      string
	multicore bool
	stats     *Stats
	bus       *Bus
}

func (server *GosheniteServer) OnBoot(eng gnet.Engine) gnet.Action {
	server.eng = eng
	log.Info("Server started: listening on ", server.addr)
	return gnet.None
}

func (server *GosheniteServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	server.stats.Record("tcp", "connections")
	return nil, gnet.None
}

func (server *GosheniteServer) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)

	dps, _ := ParsePlainGraphiteProtocol(buf)
	for _, dp := range dps {
		server.bus.Emit(&dp)
	}
	return gnet.None
}

func (server *GosheniteServer) Shutdown(ctx context.Context) {
	log.Info("Shutting down server...")
	server.eng.Stop(ctx)
}
