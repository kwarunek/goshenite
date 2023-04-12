package main

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gocql/gocql"
)

type IStore interface {
	Insert(*DataPoint) error
}

type CassandraStore struct {
	session             *gocql.Session
	cluster             *gocql.ClusterConfig
	resolutionInSeconds int64
	retentionInSeconds  int64
	query               string
	stats               *Stats
}

func (s *CassandraStore) Insert(datapoint *DataPoint) error {
	if datapoint.Timestamp < 1 {
		datapoint.Timestamp = time.Now().Unix()
	}
	resTs := (datapoint.Timestamp / s.resolutionInSeconds) * s.resolutionInSeconds
	err := s.session.Query(s.query, datapoint.Value, datapoint.Metric, resTs).Exec()
	if err != nil {
		log.Error("Failed inserting data into Cassandra:", err)
		s.stats.Record("cassandra", "store.failed")
		return err
	}
	s.stats.Record("cassandra", "store.success")
	return nil
}

func (s *CassandraStore) connect() error {
	var err error
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		return err
	}
	return nil
}

func NewCassandraStore(config *StoreConfig, stats *Stats) (IStore, error) {
	res := int64(ParseDurationWithFallback(config.Resolution, time.Second*60).Seconds())
	ret := int64(ParseDurationWithFallback(config.Retention, time.Hour*24).Seconds())
	cluster := gocql.NewCluster()
	cluster.Hosts = config.Hosts
	cluster.Port = config.Port
	cluster.Keyspace = config.Keyspace
	if config.Password != "" && config.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 3, Min: 3, Max: 90}
	store := &CassandraStore{
		session:             nil,
		cluster:             cluster,
		query:               fmt.Sprintf(`UPDATE %s USING TTL %d SET value = ? WHERE path=? AND timestamp=?`, config.Table, ret),
		retentionInSeconds:  int64(ret),
		resolutionInSeconds: int64(res),
		stats:               stats,
	}

	err := store.connect()
	return store, err
}

func NewStore(config *StoreConfig, stats *Stats) (IStore, error) {
	switch config.Driver {
	case "cassandra":
		return NewCassandraStore(config, stats)
	default:
		return &DevNull{}, nil
	}
}
