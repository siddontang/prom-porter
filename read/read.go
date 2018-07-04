package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/siddontang/prom-porter/util"

	"github.com/dgraph-io/badger"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func isMatched(m *prompb.LabelMatcher, value string) bool {
	switch m.Type {
	case prompb.LabelMatcher_EQ:
		return m.Value == value
	case prompb.LabelMatcher_NEQ:
		return m.Value != value
	case prompb.LabelMatcher_RE:
		re, err := regexp.Compile("^(?:" + m.Value + ")$")
		if err != nil {
			log.Fatalf("create regexp for %s failed %v", m.Value, err)
		}
		return re.MatchString(value)
	case prompb.LabelMatcher_NRE:
		re, err := regexp.Compile("^(?:" + m.Value + ")$")
		if err != nil {
			log.Fatalf("create regexp for %s failed %v", m.Value, err)
		}
		return !re.MatchString(value)
	}

	return false
}

func getMetricLabelNameMatcher(matchers []*prompb.LabelMatcher) *prompb.LabelMatcher {
	for _, m := range matchers {
		if m.Name == model.MetricNameLabel {
			return m
		}
	}

	log.Fatalf("must provide a matcher for %s", model.MetricNameLabel)
	return nil
}

func checkMatcher(matchers []*prompb.LabelMatcher, metric model.Metric) bool {
	for _, m := range matchers {
		if !isMatched(m, string(metric[model.LabelName(m.Name)])) {
			return false
		}
	}

	return true
}

func doQuery(db *badger.DB, q *prompb.Query) (*prompb.QueryResult, error) {
	start := q.StartTimestampMs
	end := q.EndTimestampMs

	timeSeries := map[string]*prompb.TimeSeries{}

	labelNameMatcher := getMetricLabelNameMatcher(q.Matchers)

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(util.EncodeKey(start, 0, nil))

		for ; it.Valid(); it.Next() {
			item := it.Item()
			ts, _, key := util.DecodeKey(item.Key())
			if ts > end {
				break
			}

			if !isMatched(labelNameMatcher, string(key)) {
				continue
			}

			item1, err := txn.Get(item.Key())
			if err != nil {
				return err
			}

			value, err := item1.Value()
			if err != nil {
				return err
			}

			var sample model.Sample
			err = json.Unmarshal(value, &sample)
			if err != nil {
				return err
			}

			metricKey := sample.Metric.String()
			series, ok := timeSeries[metricKey]
			if !ok {
				if !checkMatcher(q.Matchers, sample.Metric) {
					continue
				}

				series = &prompb.TimeSeries{}
				timeSeries[metricKey] = series

				labelNames := make([]string, 0, len(sample.Metric))
				for k := range sample.Metric {
					labelNames = append(labelNames, string(k))
				}
				sort.Strings(labelNames) // Sort for unittests.
				for _, k := range labelNames {
					series.Labels = append(series.Labels,
						&prompb.Label{
							Name:  string(k),
							Value: string(sample.Metric[model.LabelName(k)])})
				}
			}

			series.Samples = append(series.Samples,
				&prompb.Sample{
					Value:     float64(sample.Value),
					Timestamp: int64(sample.Timestamp)})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(timeSeries))
	for k := range timeSeries {
		names = append(names, k)
	}

	sort.Strings(names)

	r := &prompb.QueryResult{}

	for _, name := range names {
		r.Timeseries = append(r.Timeseries, timeSeries[name])
	}

	return r, nil
}

func runQuery(db *badger.DB, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resps := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, 0, len(req.Queries)),
	}
	for _, query := range req.Queries {
		resp, err := doQuery(db, query)
		if err != nil {
			return nil, err
		}

		resps.Results = append(resps.Results, resp)
	}

	return resps, nil
}

func handleRead(db *badger.DB) {
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = runQuery(db, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("must provide a storage path")
	}

	opts := badger.DefaultOptions
	opts.Dir = os.Args[1]
	opts.ValueDir = opts.Dir
	opts.ReadOnly = true

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	handleRead(db)

	http.ListenAndServe(":1235", nil)
}
