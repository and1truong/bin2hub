package pkg

import (
	"context"
	
	"github.com/andytruong/bin2hub/pkg/position"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func NewApplication(cnf *Config, logger *zap.Logger) (*Application, error) {
	positionService, err := position.NewPositionService(cnf.BinLogReader.Position.Directory, cnf.BinLogReader.Position.Name, logger)
	if err != nil {
		return nil, err
	}
	
	reader, err := newBinlogReader(cnf, positionService, logger)
	if nil != err {
		return nil, err
	}
	
	return &Application{
		cnf:      cnf,
		logger:   logger,
		reader:   reader,
		position: positionService,
	}, nil
}

type (
	Application struct {
		cnf      *Config
		reader   *binlogReader
		logger   *zap.Logger
		position *position.PositionService
	}
	
	Event struct {
		Database  string                 `json:"database"`
		Table     string                 `json:"table"`
		Action    string                 `json:"action"`
		Payload   map[string]interface{} `json:"payload"`
		Timestamp int64                  `json:"timestamp"`
	}
)

func (this *Application) Run(ctx context.Context) error {
	eg := errgroup.Group{}
	
	eg.Go(func() error {
		<-ctx.Done()
		
		return ctx.Err()
	})
	
	eg.Go(
		func() error {
			return this.position.Run(ctx)
		},
	)
	
	eg.Go(func() error {
		return this.reader.run(ctx, this.cnf.Connection)
	})
	
	return eg.Wait()
}
