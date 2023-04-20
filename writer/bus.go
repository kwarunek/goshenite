package main

import (
	"context"
	"time"

	lane "github.com/oleiade/lane/v2"
	log "github.com/sirupsen/logrus"
)

type Bus struct {
	config  *BusConfig
	store   IStore
	index   IIndex
	queue   *lane.Queue[*DataPoint]
	running bool
	stats   *Stats
}

func (b *Bus) Emit(datapoint *DataPoint) {
	b.stats.RecordMetricIngestion(datapoint.Metric)
	b.stats.Record("bus", "queue.added")
	if b.config.Queued {
		b.queue.Enqueue(datapoint)
	} else {
		b.put(datapoint)
	}

}

func (b *Bus) Stop() {
	b.running = false
}

func (b *Bus) Start() {
	b.running = true
	go b.run()
	go b.stats.Start(b.Emit)
}

func (b *Bus) Drain(ctx context.Context) {
	log.Info("Draining bus (metric queue)...")
	drainedStats := false
	if b.config.Queued {
		for {
			datapoint, ok := b.queue.Dequeue()
			if !ok {
				if drainedStats {
					break
				} else {
					log.Info("Draining stats...")
					b.stats.Drain()
					drainedStats = true
				}
				continue
			}
			b.store.Insert(datapoint)
			b.index.Index(datapoint)
		}
	}
	b.index.Shutdown(ctx)

}

func (b *Bus) run() {
	log.Info("Bus started")
	statsDumpTs := time.Now().Unix()
	for b.running {
		var datapoint *DataPoint
		ok := false
		if b.config.Queued {
			datapoint, ok = b.queue.Dequeue()
			curTs := time.Now().Unix()
			if (curTs - statsDumpTs) > 59 {
				b.stats.RecordFixed("bus", "queue.size", int64(b.queue.Size()))
				statsDumpTs = curTs
			}
		}
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		b.put(datapoint)
		b.stats.Record("bus", "queue.processed")
	}
}

func (b *Bus) put(datapoint *DataPoint) {
	b.store.Insert(datapoint)
	b.index.Index(datapoint)
}

func NewBus(store IStore, index IIndex, stats *Stats, config *BusConfig) *Bus {
	bus := &Bus{
		config:  config,
		index:   index,
		store:   store,
		running: false,
		stats:   stats,
		queue:   lane.NewQueue[*DataPoint](),
	}
	return bus
}
