package main

import (
	log "github.com/sirupsen/logrus"
)

type DevNull struct{}

func (d *DevNull) Index(datapoint *DataPoint) error {
	log.Debug("index:/dev/null: ", datapoint)
	return nil
}

func (d *DevNull) Insert(datapoint *DataPoint) error {
	log.Debug("insert:/dev/null: ", datapoint)
	return nil
}
