package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/panjf2000/gnet/v2"
)

type gosheniteServer struct {
	gnet.BuiltinEventEngine

	eng       gnet.Engine
	addr      string
	multicore bool
	stats     *Stats
	bus       *Bus
}

func (server *gosheniteServer) OnBoot(eng gnet.Engine) gnet.Action {
	server.eng = eng
	log.Printf("Goshenite is listening on %s\n", server.addr)
	return gnet.None
}

func (server *gosheniteServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	go server.stats.Record("tcp", "connections")
	return nil, gnet.None
}

func (server *gosheniteServer) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)

	dps, _ := ParsePlainGraphiteProtocol(buf)
	for _, dp := range dps {
		server.bus.Emit(&dp)
	}
	return gnet.Close
}
