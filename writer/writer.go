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

	// Set up store
	store, err := NewStore(config.Store)
	if err != nil {
		log.Fatal("Cannot initialize store connection:", err)

	}
	// Set up index
	index, err := NewIndex(config.Index)
	if err != nil {
		log.Fatal("Cannot initialize index connection:", err)
	}

	// Set up stats
	stats := NewStats(config.Stats)
	go stats.Start()

	bus := NewBus(store, index)
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
