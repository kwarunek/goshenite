package main

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	opensearch "github.com/opensearch-project/opensearch-go/v2"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
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
	cache       *lru.ARCCache[string, int64]
	config      *IndexConfig
	client      *opensearch.Client
	bulkIndexer opensearchutil.BulkIndexer
	lastStats   opensearchutil.BulkIndexerStats
	stats       *Stats
}

type PathDoc struct {
	path  string
	leaf  bool
	depth int
}

func MD5Sum(input string) string {
	hash := md5.New()
	hash.Write([]byte(input))
	hashSum := hash.Sum(nil)
	return hex.EncodeToString(hashSum)
}

func (idx *OpensearchIndex) exists(metric string) bool {
	getter := opensearchapi.GetRequest{Index: idx.config.Name, DocumentID: MD5Sum(metric)}
	res, err := getter.Do(context.Background(), idx.client)
	if res.Body != nil {
		defer res.Body.Close()
	}
	ret := (err == nil && res.StatusCode == 200)
	if err != nil {
		idx.stats.Record("index", "check_exits.error")
	}
	return ret
}

func (idx *OpensearchIndex) isCached(metric string) bool {
	val, ok := idx.cache.Get(metric)
	return ok && (val == 1)
}

func (idx *OpensearchIndex) Index(datapoint *DataPoint) error {
	if idx.isCached(datapoint.Metric) {
		idx.stats.Record("index", "cache.hit")
		// if there is a whole metric in the cache dont even try with a subpath
		return nil
	}

	segments := strings.Split(datapoint.Metric, ".")

	for i, j := 1, len(segments); i <= j; i++ {
		metric := strings.Join(segments[:i], ".")
		isLeaf := i == j
		// TODO: use mget
		if idx.isCached(metric) {
			idx.stats.Record("index", "cache.hit")
		} else {
			idx.stats.Record("index", "cache.miss")
			if !idx.exists(metric) {
				idx.add(PathDoc{depth: i, leaf: isLeaf, path: metric})
			} else {
				idx.stats.Record("index", "doc.already_in")
			}
			idx.cache.Add(metric, 1)
		}
	}
	return nil
}

func (idx *OpensearchIndex) flushEnd(ctx context.Context) {
	ws := idx.bulkIndexer.Stats()
	v := reflect.ValueOf(ws)
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		lv := reflect.ValueOf(idx.lastStats).FieldByName(t.Field(i).Name).Uint()
		idx.stats.Record("index", t.Field(i).Name, int64(v.Field(i).Interface().(uint64))-int64(lv))
	}
	idx.lastStats = ws
	idx.stats.Record("index", "flush")
	idx.stats.RecordFixed("index", "cache.size", int64(idx.cache.Len()))
}

func (idx *OpensearchIndex) Shutdown(ctx context.Context) {
	idx.bulkIndexer.Close(ctx)
}

func (idx *OpensearchIndex) add(doc PathDoc) {
	// set dummy tenant for disthene-compat
	jdoc := fmt.Sprintf(`{"depth": %d, "tenant": "NONE", "leaf": %t, "path": "%s"}`, doc.depth, doc.leaf, doc.path)

	err := idx.bulkIndexer.Add(
		context.Background(),
		opensearchutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: MD5Sum(doc.path),
			Body:       strings.NewReader(jdoc),
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

	tlsTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: bool(config.Insecure)},
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 5 * time.Second,
		DisableKeepAlives:   true,
	}

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: config.Addresses,
		Signer:    signer,
		Transport: tlsTransport,
		Username:  config.Username,
		Password:  config.Password,

		// TODO: should be configurable
		RetryBackoff:         func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:           3,
		EnableRetryOnTimeout: true,
		EnableMetrics:        true,
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
	oi := &OpensearchIndex{
		cache:       nil,
		config:      config,
		client:      nil,
		bulkIndexer: nil,
		lastStats:   opensearchutil.BulkIndexerStats{0, 0, 0, 0, 0, 0, 0, 0},
		stats:       stats,
	}

	oi.cache, err = lru.NewARC[string, int64](config.Cache.Size)
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
