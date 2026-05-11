package xboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sagernet/bbolt"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service/filemanager"

	"github.com/gofrs/uuid/v5"
)

type trafficJournal struct {
	db *bbolt.DB
}

type trafficBatch struct {
	Stream   string
	Sequence int64
	Traffic  map[string][2]int64
	Queued   map[string][2]int64
}

func openTrafficJournal(ctx context.Context, tag string) (*trafficJournal, error) {
	digest := sha256.Sum256([]byte(tag))
	name := fmt.Sprintf("xboard-traffic-%x.db", digest[:8])
	file, err := filemanager.OpenFile(ctx, name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	db, err := bbolt.Open(file.Name(), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	return &trafficJournal{db}, nil
}

var trafficBucket = []byte("reports")

func trafficKey(n *node) []byte {
	return fmt.Appendf(nil, "%q/%q/%d/%q", n.panel.options.URL, n.options.Panel, n.options.NodeID, n.options.Inbound)
}

func loadTraffic(tx *bbolt.Tx, key []byte, batch *trafficBatch) error {
	if bucket := tx.Bucket(trafficBucket); bucket != nil {
		if raw := bucket.Get(key); raw != nil {
			return json.Unmarshal(raw, batch)
		}
	}
	return nil
}

func (j *trafficJournal) update(n *node, change func(*trafficBatch) error) (*trafficBatch, error) {
	key := trafficKey(n)
	var batch trafficBatch
	err := j.db.Update(func(tx *bbolt.Tx) error {
		if err := loadTraffic(tx, key, &batch); err != nil {
			return err
		}
		if err := change(&batch); err != nil {
			return err
		}
		data, err := json.Marshal(&batch)
		if err != nil {
			return err
		}
		bucket, err := tx.CreateBucketIfNotExists(trafficBucket)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
	return &batch, err
}

func (s *Service) stageTraffic(n *node) (*trafficBatch, error) {
	traffic := s.tracker.readTraffic(n.options.Inbound)
	if len(traffic) == 0 {
		var batch trafficBatch
		err := s.journal.db.View(func(tx *bbolt.Tx) error {
			return loadTraffic(tx, trafficKey(n), &batch)
		})
		if err != nil || len(batch.Traffic) > 0 || len(batch.Queued) == 0 {
			return &batch, err
		}
	}
	batch, err := s.journal.update(n, func(batch *trafficBatch) error {
		if batch.Stream == "" {
			id, err := uuid.NewV4()
			if err != nil {
				return err
			}
			batch.Stream = id.String()
		}
		if batch.Queued == nil {
			batch.Queued = make(map[string][2]int64)
		}
		for uid, delta := range traffic {
			queued := batch.Queued[uid]
			batch.Queued[uid] = [2]int64{queued[0] + delta[0], queued[1] + delta[1]}
		}
		if len(batch.Traffic) == 0 {
			batch.Sequence++
			batch.Traffic, batch.Queued = batch.Queued, nil
		}
		return nil
	})
	if err != nil {
		s.tracker.addTraffic(n.options.Inbound, traffic)
	}
	return batch, err
}

type reportReceipt struct {
	Stream   string `json:"report_stream"`
	Sequence int64  `json:"report_seq"`
}

type reportPart struct {
	reportReceipt
	OK   bool `json:"ok"`
	Code int  `json:"code"`
}

func (part *reportPart) check(name string) error {
	if part == nil {
		return E.New("missing xboard ", name, " report result")
	}
	if !part.OK {
		return E.New("xboard ", name, " report rejected: ", part.Code)
	}
	return nil
}

type reportResponse struct {
	reportReceipt
	Data    bool `json:"data"`
	Results struct {
		Traffic *reportPart `json:"traffic"`
		Status  *reportPart `json:"status"`
	} `json:"results"`
}

func (r reportResponse) check(capabilities reportCapabilities, batch *trafficBatch) (trafficErr, statusErr error) {
	receipt := r.reportReceipt
	if capabilities.Parts {
		statusErr = r.Results.Status.check("status")
		if len(batch.Traffic) == 0 {
			return nil, statusErr
		}
		if err := r.Results.Traffic.check("traffic"); err != nil {
			return err, statusErr
		}
		receipt = r.Results.Traffic.reportReceipt
	}
	if len(batch.Traffic) > 0 && (capabilities.Parts || capabilities.Ack || receipt.Stream != "" || receipt.Sequence != 0) {
		if receipt.Stream != batch.Stream || receipt.Sequence != batch.Sequence {
			return E.New("unexpected xboard traffic acknowledgement"), statusErr
		}
	}
	return nil, statusErr
}
