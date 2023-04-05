// utils
package main

import (
	"time"

	log "github.com/sirupsen/logrus"
)

func ParseDurationWithFallback(s string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Warn(err)
		return fallback
	}
	return d
}
