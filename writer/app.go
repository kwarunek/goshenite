// app
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/panjf2000/gnet/v2"
)

type App struct {
	sync.RWMutex
	bus    *Bus
	server *GosheniteServer
	config *Config
	exit   chan bool
}

func NewApp(config *Config) *App {
	stats := NewStats(config.Stats)

	store, err := NewStore(config.Store, stats)
	if err != nil {
		log.Fatal("Cannot initialize store connection:", err)
	}
	log.Info("Store inited: ", config.Store.Driver)

	index, err := NewIndex(config.Index, stats)

	if err != nil {
		log.Fatal("Cannot initialize index connection:", err)
	}
	log.Info("Index inited: ", config.Index.Driver)

	bus := NewBus(store, index, stats)

	server := &GosheniteServer{
		addr:  fmt.Sprintf("tcp://:%d", config.Endpoint.Port),
		stats: stats,
		bus:   bus,
	}
	app := &App{
		config: config,
		server: server,
		bus:    bus,
		exit:   make(chan bool),
	}
	return app

}

func (app *App) Start() {
	app.bus.Start()

	go func() {
		err := gnet.Run(
			app.server, app.server.addr,
			gnet.WithMulticore(app.config.Endpoint.Multicore),
			gnet.WithReusePort(app.config.Endpoint.Reuseport),
		)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func (app *App) Loop() {
	app.RLock()
	exitChan := app.exit
	app.RUnlock()

	if exitChan != nil {
		<-app.exit
	}
}
func (app *App) Shutdown(ctx context.Context) {
	log.Info("Shutting down server...")
	app.server.Shutdown(ctx)
	log.Info("Draining bus (metric queue)...")
	app.bus.Drain(ctx)
	// TODO flush Stats
	log.Info("Waiting to auto-flush stats...")
	time.Sleep(1 * time.Minute)
	if app.exit != nil {
		close(app.exit)
		app.exit = nil
	}
}
