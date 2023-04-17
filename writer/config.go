// config
package main

import (
	"github.com/sherifabdlnaby/configuro"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Index    *IndexConfig
	Store    *StoreConfig
	Endpoint *EndpointConfig
	Stats    *StatsConfig
	Logger   *LoggerConfig
}
type EndpointConfig struct {
	Port      int
	Multicore bool
	Reuseport bool
}
type StoreConfig struct {
	Driver     string
	Hosts      []string
	Port       int
	Retention  string
	Resolution string
	Keyspace   string
	Username   string
	Password   string
	Table      string
}
type IndexConfig struct {
	Driver    string
	Addresses []string
	Name      string
	Password  string
	Username  string
	Insecure  bool
	Region    string
	Sigv4     bool
	Flush     struct {
		Bytes    int
		Interval string // duration X[m,s,h]
	}
	Cache struct {
		Shards      int
		Lifewindow  string // duration
		Cleanwindow string // duration
		Size        int    // in bytes max cache size
	}
}
type StatsConfig struct {
	Path     string
	Interval string // 60s
	Log      bool
	Segment  int
}
type LoggerConfig struct {
	Level string
}

func PrepareConfig(configFile string) *Config {
	Loader, err := configuro.NewConfig(configuro.WithLoadFromConfigFile(configFile, true))
	if err != nil {
		log.Fatal(err)
	}
	config := &Config{}
	err = Loader.Load(config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}
