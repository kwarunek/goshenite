package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/allegro/bigcache/v3"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	opensearch "github.com/opensearch-project/opensearch-go/v2"
	opensearchutil "github.com/opensearch-project/opensearch-go/v2/opensearchutil"
	"github.com/opensearch-project/opensearch-go/v2/signer"
	requestsigner "github.com/opensearch-project/opensearch-go/v2/signer/awsv2"

	log "github.com/sirupsen/logrus"
)

type IIndex interface {
	Index(datapoint *DataPoint) error
	Shutdown(ctx context.Context)
}

type OpensearchIndex struct {
	// client *elasticsearch.Client
	cache       *bigcache.BigCache
	client      *opensearch.Client
	bulkIndexer opensearchutil.BulkIndexer
	stats       *Stats
}

type PathDoc struct {
	path  string
	leaf  bool
	depth int
}

func (idx *OpensearchIndex) Index(datapoint *DataPoint) error {
	segments := strings.Split(datapoint.Metric, ".")
	if cached, err := idx.cache.Get(datapoint.Metric); len(cached) > 0 || err == nil {
		log.Debug("HIT Metric cache: ", datapoint.Metric)
	} else {

		for i, j := 1, len(segments); i <= j; i++ {
			metric := strings.Join(segments[:i], ".")
			isLeaf := i == j
			idx.add(&PathDoc{depth: i, leaf: isLeaf, path: metric})
			idx.cache.Set(metric, []byte{})
		}
	}
	return nil
}

func (idx *OpensearchIndex) flushEnd(ctx context.Context) {
	ws := idx.bulkIndexer.Stats()
	v := reflect.ValueOf(ws)
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		idx.stats.Record("index", t.Field(i).Name, int64(v.Field(i).Interface().(uint64)))
	}
}

func (idx *OpensearchIndex) Shutdown(ctx context.Context) {
	idx.bulkIndexer.Close(ctx)
}

func (idx *OpensearchIndex) add(doc *PathDoc) {
	jdoc := fmt.Sprintf(`{"depth": %d, "leaf": %t, "path": "%s"}`, doc.depth, doc.leaf, doc.path)

	err := idx.bulkIndexer.Add(
		context.Background(),
		opensearchutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: doc.path,
			Body:       strings.NewReader(jdoc),
			OnFailure: func(
				ctx context.Context,
				item opensearchutil.BulkIndexerItem,
				res opensearchutil.BulkIndexerResponseItem, err error,
			) {
				if err != nil {
					log.Error("ERROR: ", err)
				} else {
					log.Error("ERROR: ", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		log.Error("Unexpected error: %s", err)
	}
}

func NewOpenSearch(config *IndexConfig, onFlushEnd func(context.Context)) (*opensearch.Client, opensearchutil.BulkIndexer, error) {
	ctx := context.Background()
	var signer signer.Signer
	var tlsTransport *http.Transport

	if config.Sigv4 {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithDefaultRegion(config.Region))
		if err != nil {
			return nil, nil, err
		}
		signer, err = requestsigner.NewSignerWithService(awsCfg, "es")
		if err != nil {
			return nil, nil, err
		}
	}

	if config.Insecure {
		tlsTransport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: config.Addresses,
		Signer:    signer,
		Transport: tlsTransport,
		Username:  config.Username,
		Password:  config.Password,

		// TODO: should be configurable
		RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:    1,
		EnableMetrics: true,
	})
	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}
	bulkIndexer, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client:        client,
		Index:         config.Name,
		FlushBytes:    config.Flush.Bytes,
		FlushInterval: ParseDurationWithFallback(config.Flush.Interval, 5*time.Minute),
		OnError:       func(context.Context, error) { log.Error("OpenSearch ", err) },
		OnFlushEnd:    onFlushEnd,
	})

	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}
	return client, bulkIndexer, nil
}

func NewOpensearchIndex(config *IndexConfig, stats *Stats) (IIndex, error) {
	var err error
	oi := &OpensearchIndex{cache: nil, client: nil, bulkIndexer: nil, stats: stats}

	cacheConfig := bigcache.Config{
		// TODO: should be configurable
		Shards:           1024,
		LifeWindow:       10 * time.Minute,
		CleanWindow:      5 * time.Minute,
		MaxEntrySize:     500,
		HardMaxCacheSize: 0,
	}
	oi.cache, err = bigcache.New(context.Background(), cacheConfig)
	if err != nil {
		return nil, err
	}
	oi.client, oi.bulkIndexer, err = NewOpenSearch(config, oi.flushEnd)
	if err != nil {
		return nil, err
	}
	return oi, nil
}

func NewIndex(config *IndexConfig, stats *Stats) (IIndex, error) {
	switch config.Driver {
	case "opensearch":
		return NewOpensearchIndex(config, stats)
	default:
		return &DevNull{}, nil
	}
}
