package main

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/dgraph-io/badger"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func encodeKey(ts int64, c int64) []byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(ts))
	binary.BigEndian.PutUint64(buf[8:], uint64(c))
	return buf[:]
}

func decodeKey(buf []byte) (int64, int64) {
	ts := int64(binary.BigEndian.Uint64(buf[:8]))
	c := int64(binary.BigEndian.Uint64(buf[8:]))
	return ts, c
}

func isMatched(m *prompb.LabelMatcher, metric model.Metric) bool {
	value := string(metric[model.LabelName(m.Name)])
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

func checkMatcher(matchers []*prompb.LabelMatcher, metric model.Metric) bool {
	for _, m := range matchers {
		if !isMatched(m, metric) {
			return false
		}
	}

	return true
}

func doQuery(db *badger.DB, q *prompb.Query) (*prompb.QueryResult, error) {
	start := q.StartTimestampMs
	end := q.EndTimestampMs

	timeSeries := map[string]*prompb.TimeSeries{}

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(encodeKey(start, 0))

		for ; it.Valid(); it.Next() {
			item := it.Item()
			ts, _ := decodeKey(item.Key())
			if ts > end {
				break
			}

			value, err1 := item.Value()
			if err1 != nil {
				return err1
			}

			var sample model.Sample
			err1 = json.Unmarshal(value, &sample)
			if err1 != nil {
				return err1
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
