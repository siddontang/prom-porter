package main

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var counter int64 = 0

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

func getLastCounter(db *badger.DB) {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(encodeKey(math.MaxInt64, math.MaxInt64))
		if it.Valid() {
			item := it.Item()
			_, counter = decodeKey(item.Key())
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
			it.Seek(encodeKey(start, 0))
			for ; it.Valid(); it.Next() {
				item := it.Item()
				ts, _ := decodeKey(item.Key())
				if ts > end {
					return nil
				}

				// TODO: use batch
				err1 := backupDB.Update(func(txn1 *badger.Txn) error {
					value, err2 := item.Value()
					if err2 != nil {
						return err2
					}
					return txn1.Set(item.Key(), value)
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

				key := encodeKey(int64(sample.Timestamp), newCounter)
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
