package position

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"
	
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"
)

func NewPositionService(dir, name string, logger *zap.Logger) (*PositionService, error) {
	file, err := os.Create(dir + "/" + name + ".json")
	if nil != err {
		return nil, err
	}
	
	return &PositionService{
		name:     name,
		position: mysql.Position{},
		mutex:    &sync.Mutex{},
		file:     file,
		logger:   logger,
	}, nil
}

// Writing data to disk taking time
type PositionService struct {
	name     string
	position mysql.Position
	mutex    *sync.Mutex
	file     *os.File
	logger   *zap.Logger
}

func (this *PositionService) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		
		// repeat every second
		case <-time.After(1 * time.Second):
			this.mutex.Lock()
			pos := this.position
			this.mutex.Unlock()
			
			// If position changed -> persist new position to storage
			if pos.Name != "" {
				if err := this.write(pos); nil != err {
					return err
				} else {
					// Reset position property
					this.mutex.Lock()
					this.position = mysql.Position{}
					this.mutex.Unlock()
				}
			}
		}
	}
}

func (this *PositionService) Save(pos mysql.Position) error {
	this.mutex.Lock()
	this.position = pos
	this.mutex.Unlock()
	
	return nil
}

func (this *PositionService) write(pos mysql.Position) error {
	this.logger.Info(
		"PositionService.write()",
		zap.String("name", pos.Name),
		zap.Uint32("POS", pos.Pos),
	)
	
	body, err := json.Marshal(pos)
	if nil != err {
		return err
	}
	
	_, err = this.file.Write(body)
	
	return err
}

func (this *PositionService) Get() (mysql.Position, error) {
	var err error
	
	pos := mysql.Position{}
	body := []byte{}
	_, err = this.file.Read(body)
	if nil != err {
		return pos, err
	}
	
	err = json.Unmarshal(body, &pos)
	
	return pos, err
}
