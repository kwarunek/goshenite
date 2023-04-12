package main

import (
	"context"
	"time"

	lane "github.com/oleiade/lane/v2"
	log "github.com/sirupsen/logrus"
)

type Bus struct {
	store   IStore
	index   IIndex
	queue   *lane.Queue[*DataPoint]
	running bool
	stats   *Stats
}

func (b *Bus) Emit(datapoint *DataPoint) {
	b.stats.RecordMetricIngestion(datapoint.Metric)
	b.queue.Enqueue(datapoint)
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
	for {
		datapoint, ok := b.queue.Dequeue()
		if !ok {
			break
		}
		b.store.Insert(datapoint)
		b.index.Index(datapoint)
	}
	b.index.Shutdown(ctx)
}

func (b *Bus) run() {
	log.Info("Bus started")
	for b.running {
		datapoint, ok := b.queue.Dequeue()
		if !ok {

			time.Sleep(time.Second)
			continue
		}
		b.store.Insert(datapoint)
		b.index.Index(datapoint)
	}
}

func NewBus(store IStore, index IIndex, stats *Stats) *Bus {
	bus := &Bus{
		index:   index,
		store:   store,
		running: false,
		stats:   stats,
		queue:   lane.NewQueue[*DataPoint](),
	}
	return bus
}
