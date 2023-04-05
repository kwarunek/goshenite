package main

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Stats struct {
	sync.RWMutex
	metrics map[string]int64
	config  *StatsConfig
}

func (s *Stats) Record(unit string, stat string, value ...int64) {
	val := int64(1)
	if len(value) > 0 {
		val = value[0]
	}
	s.Lock()
	defer s.Unlock()
	s.metrics[s.config.Path+"."+unit+"."+stat] += val
}

func (s *Stats) Start() {
	ticker := time.NewTicker(ParseDurationWithFallback(s.config.Interval, 60*time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Flush()
		}
	}
}
func (s *Stats) Flush() {
	s.Lock()
	frozenStat := make(map[string]int64)
	for m, v := range s.metrics {
		frozenStat[m] = v
		s.metrics[m] = 0
	}
	s.Unlock()
	if len(frozenStat) > 0 {
		// TODO: emit datapoint
		log.Info(frozenStat)

	}
}

func NewStats(config *StatsConfig) *Stats {
	metrics := make(map[string]int64)
	return &Stats{metrics: metrics, config: config}
}
