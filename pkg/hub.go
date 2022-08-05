package pkg

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

func newMyHub(cnf *Config, chError chan<- error, logger *zap.Logger) (*myHub, error) {
	hub, err := eventhub.NewHubFromConnectionString(cnf.Connection.EventHub.ConnectionUrl)
	if nil != err {
		return nil, err
	}
	
	return &myHub{
		mutex:    &sync.Mutex{},
		logger:   logger,
		hub:      hub,
		events:   []Event{},
		maxLen:   cnf.Connection.EventHub.Publishing.MaxEvents,
		interval: cnf.Connection.EventHub.Publishing.Interval,
		chError:  chError,
	}, nil
}

type myHub struct {
	mutex    *sync.Mutex
	logger   *zap.Logger
	hub      *eventhub.Hub
	events   []Event
	maxLen   int
	interval time.Duration
	chError  chan<- error
}

func (this *myHub) append(ctx context.Context, event Event) error {
	this.logger.Info("append", zap.Any("data", event))
	defer this.logger.Info("append done")
	this.mutex.Lock()
	this.events = append(this.events, event)
	this.mutex.Unlock()
	
	if len(this.events) > this.maxLen {
		return this.flush(ctx)
	}
	
	if len(this.events) == 1 {
		go func() {
			<-time.After(this.interval)
			_ = this.flush(ctx)
		}()
	}
	
	return nil
}

func (this *myHub) flush(ctx context.Context) error {
	this.mutex.Lock()
	defer func() {
		this.events = []Event{}
		this.mutex.Unlock()
	}()
	defer this.logger.Info("flush done")
	
	events := []*eventhub.Event{}
	
	for _, e := range this.events {
		if body, err := json.Marshal(e); nil != err {
			return err
		} else {
			event := eventhub.NewEvent(body)
			event.PartitionKey = stringPointer(e.Database, ":", e.Table)
			events = append(events, event)
		}
	}
	
	if len(events) > 0 {
		start := time.Now()
		batch := eventhub.NewEventBatchIterator(events...)
		err := this.hub.SendBatch(ctx, batch)
		if nil != err {
			return errors.Wrap(err, "eventhub batch sending error")
		}

		duration := time.Since(start)
		this.logger.Info("flush", zap.Int("length", len(events)), zap.Duration("took", duration))
	}
	
	return nil
}
