package pkg

import (
	"context"
	"crypto/tls"
	"time"
	
	"github.com/andytruong/bin2hub/pkg/position"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func newBinlogReader(cnf *Config, positionService *position.PositionService, logger *zap.Logger) (
	*binlogReader, error,
) {
	chError := make(chan error)
	myHub, err := newMyHub(cnf, chError, logger)
	if nil != err {
		return nil, err
	}
	
	return &binlogReader{
		position: positionService,
		logger:   logger,
		tables: func() map[string]bool {
			tables := map[string]bool{}
			for db, tableNames := range cnf.BinLogReader.Tables {
				for _, table := range tableNames {
					tables[db+"."+table] = true
				}
			}
			
			return tables
		}(),
		myHub:   myHub,
		chError: chError,
	}, nil
}

type binlogReader struct {
	canal.DummyEventHandler
	replication.BinlogParser
	
	position *position.PositionService
	logger   *zap.Logger
	tables   map[string]bool
	myHub    *myHub
	chError  <-chan error
}

func (this *binlogReader) run(ctx context.Context, cnf ConnectionConfig) error {
	eg := errgroup.Group{}
	
	eg.Go(func() error {
		<-ctx.Done()
		return ctx.Err()
	})
	
	eg.Go(func() error {
		cfg := canal.NewDefaultConfig()
		cfg.Addr = cnf.MySQL.Address
		cfg.User = cnf.MySQL.User
		cfg.Password = cnf.MySQL.Password
		cfg.Dump.ExecutionPath = ""
		if cnf.MySQL.EnabledSSL {
			cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
		
		myCanal, err := canal.NewCanal(cfg)
		if err != nil {
			return errors.Wrap(err, "can not create new Canal")
		}
		
		// Read server binlog info
		pos, err := myCanal.GetMasterPos()
		if err != nil {
			return errors.Wrap(err, "can not get master position")
		}
		
		pos.Pos = 0 // from beginning
		myCanal.SetEventHandler(this)
		
		return myCanal.RunFrom(pos)
	})
	
	eg.Go(func() error {
		return <-this.chError
	})
	
	return eg.Wait()
}

func (this *binlogReader) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return this.position.Save(pos)
}

func (this *binlogReader) OnRow(e *canal.RowsEvent) error {
	ctx := context.Background()
	
	// Don't sync all events, only flagged tables.
	if _, found := this.tables[e.Table.Schema+"."+e.Table.Name]; !found {
		return nil
	}
	
	// base value for canal.DeleteAction or canal.InsertAction
	var rowFrom = 0
	var rowInc = 1
	
	if e.Action == canal.UpdateAction {
		rowFrom = 1
		rowInc = 2
	}
	
	if e.Action != canal.UpdateAction && e.Action != canal.DeleteAction && e.Action != canal.InsertAction {
		return nil // skip
	}
	
	for i := rowFrom; i < len(e.Rows); i += rowInc {
		payload := map[string]interface{}{}
		for j, column := range e.Table.Columns {
			payload[column.Name] = e.Rows[i][j]
		}
		
		err := this.myHub.append(ctx, Event{
			Database:  e.Table.Schema,
			Table:     e.Table.Name,
			Action:    e.Action,
			Payload:   payload,
			Timestamp: time.Now().Unix(),
		})
		
		if nil != err {
			return err
		}
	}
	
	return nil
}
