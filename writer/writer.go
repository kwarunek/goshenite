package main

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
)

var config *Config

func main() {
	// Set up config
	var configFile string
	flag.StringVar(&configFile, "c", "conf/config.yaml", "-c /etc/goshenite/config.yaml")
	flag.Parse()
	config := PrepareConfig(configFile)

	level, err := log.ParseLevel(config.General.Level)
	if err != nil {
		level = log.InfoLevel
	}
	log.SetLevel(level)

	if config.General.Profiler {
		defer profile.Start(profile.MemProfile).Stop()
		go func() {
			http.ListenAndServe(":9999", nil)
		}()
	}

	app := NewApp(config)
	app.Start()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		for {
			<-c
			app.Shutdown(context.Background())
			os.Exit(0)
		}
	}()

	app.Loop()
}
