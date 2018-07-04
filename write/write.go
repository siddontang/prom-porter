package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/siddontang/prom-porter/util"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var counter int64 = 0

func getLastCounter(db *badger.DB) {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(util.EncodeKey(math.MaxInt64, math.MaxInt64, nil))
		if it.Valid() {
			item := it.Item()
			_, counter, _ = util.DecodeKey(item.Key())
		}
		return nil
	})

	if err != nil {
		log.Fatalf("get last count failed %v", err)
	}
}

func handleDump(db *badger.DB) {
	http.HandleFunc("/dump", func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.ParseInt(r.FormValue("start"), 10, 64)
		end, _ := strconv.ParseInt(r.FormValue("end"), 10, 64)
		if end == 0 {
			end = math.MaxInt64
		}

		os.RemoveAll("./test_backup")
		os.MkdirAll("./test_backup", 0755)
		opts := badger.DefaultOptions
		opts.Dir = "./test_backup"
		opts.ValueDir = opts.Dir

		backupDB, err := badger.Open(opts)
		if err != nil {
			log.Fatalf("create backup failed %v", err)
		}
		defer backupDB.Close()

		err = db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			it.Seek(util.EncodeKey(start, 0, nil))

			keys := make([][]byte, 0, 1024)
			values := make([][]byte, 0, 1024)
			for ; it.Valid(); it.Next() {
				item := it.Item()
				ts, _, _ := util.DecodeKey(item.Key())
				if ts > end {
					return nil
				}

				keys = append(keys, item.KeyCopy(nil))
				value, err := item.ValueCopy(nil)
				if err != nil {
					log.Fatalf("get %q value failed %v", item.Key(), err)
				}
				values = append(values, value)

				if len(keys) >= 1024 {
					// TODO: use batch
					err1 := backupDB.Update(func(txn1 *badger.Txn) error {
						for i := 0; i < len(keys); i++ {
							if err := txn1.Set(keys[i], values[i]); err != nil {
								return err
							}
						}

						return nil
					})

					keys = keys[0:0]
					values = values[0:0]
					if err1 != nil {
						return err1
					}
				}
			}

			if len(keys) >= 0 {
				// TODO: use batch
				err1 := backupDB.Update(func(txn1 *badger.Txn) error {
					for i := 0; i < len(keys); i++ {
						if err := txn1.Set(keys[i], values[i]); err != nil {
							return err
						}
					}

					return nil
				})

				if err1 != nil {
					return err1
				}
			}

			return nil
		})

		if err != nil {
			log.Fatalf("dump storage between timestamp [%d, %d] failed %v", start, end, err)
		}
	})
}

func handleWrite(db *badger.DB) {
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
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

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = db.Update(func(txn *badger.Txn) error {
			samples := protoToSamples(&req)
			for _, sample := range samples {
				newCounter := atomic.AddInt64(&counter, 1)
				metricName := sample.Metric[model.MetricNameLabel]

				key := util.EncodeKey(int64(sample.Timestamp), newCounter, []byte(metricName))
				buf, err := json.Marshal(&sample)
				if err != nil {
					log.Fatalf("encode metric %s failed %v", sample, err)
				}

				txn.Set(key, buf)
			}

			return nil
		})

		if err != nil {
			log.Fatalf("update storage failed %v", err)
		}
	})
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func main() {
	opts := badger.DefaultOptions
	os.MkdirAll("./test", 0755)
	opts.Dir = "./test"
	opts.ValueDir = opts.Dir

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	getLastCounter(db)

	handleDump(db)
	handleWrite(db)

	http.ListenAndServe(":1234", nil)
}
