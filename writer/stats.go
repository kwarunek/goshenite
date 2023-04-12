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
	flusher func(datapoint *DataPoint)
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

func (s *Stats) Start(flusher func(datapoint *DataPoint)) {
	s.flusher = flusher
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
	for m, v := range frozenStat {
		s.flusher(&DataPoint{Metric: m, Value: float64(v), Timestamp: 0})
		log.Debug(frozenStat)
	}

}

func NewStats(config *StatsConfig) *Stats {
	metrics := make(map[string]int64)
	return &Stats{metrics: metrics, config: config}
}
