package main

import (
	"flag"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/panjf2000/gnet/v2"
)

var config *Config

func main() {
	// Set up config
	var configFile string
	flag.StringVar(&configFile, "c", "conf/config.yaml", "-c /etc/goshenite/config.yaml")
	flag.Parse()
	config := PrepareConfig(configFile)

	stats := NewStats(config.Stats)

	// Set up store
	store, err := NewStore(config.Store, stats)
	if err != nil {
		log.Fatal("Cannot initialize store connection:", err)

	}
	// Set up index
	index, err := NewIndex(config.Index, stats)
	if err != nil {
		log.Fatal("Cannot initialize index connection:", err)
	}

	bus := NewBus(store, index, stats)
	go bus.Start()

	server := &gosheniteServer{
		addr:  fmt.Sprintf("tcp://:%d", config.Endpoint.Port),
		stats: stats,
		bus:   bus,
	}
	log.Fatal(gnet.Run(
		server, server.addr,
		gnet.WithMulticore(config.Endpoint.Multicore),
		gnet.WithReusePort(config.Endpoint.Reuseport),
	))
}
