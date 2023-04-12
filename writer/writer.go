package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
)

var config *Config

func main() {
	// Set up config
	var configFile string
	flag.StringVar(&configFile, "c", "conf/config.yaml", "-c /etc/goshenite/config.yaml")
	flag.Parse()
	config := PrepareConfig(configFile)

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
