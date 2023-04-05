package main

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gocql/gocql"
)

type Store struct {
	session             *gocql.Session
	cluster             *gocql.ClusterConfig
	resolutionInSeconds int64
	retentionInSeconds  int64
	query               string
}

func (s *Store) Insert(datapoint *DataPoint) error {
	if datapoint.Timestamp < 1 {
		datapoint.Timestamp = time.Now().Unix()
	}
	resTs := (datapoint.Timestamp / s.resolutionInSeconds) * s.resolutionInSeconds
	err := s.session.Query(s.query, datapoint.Value, datapoint.Metric, resTs).Exec()
	if err != nil {
		log.Error("Failed inserting data into Cassandra:", err)
		return err
	}
	return nil
}

func (s *Store) connect() error {
	var err error
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		return err
	}
	return nil
}

func NewStore(config *StoreConfig) (*Store, error) {
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
	store := &Store{
		session:             nil,
		cluster:             cluster,
		query:               fmt.Sprintf(`UPDATE %s USING TTL %d SET value = ? WHERE path=? AND timestamp=?`, config.Table, ret),
		retentionInSeconds:  int64(ret),
		resolutionInSeconds: int64(res),
	}

	err := store.connect()
	return store, err
}
