package pkg

import (
	"io/ioutil"
	"os"
	"time"
	
	"gopkg.in/yaml.v3"
)

func NewConfig(path string) (*Config, error) {
	raw, err := ioutil.ReadFile(path)
	
	if nil != err {
		return nil, err
	}
	
	parsed := os.ExpandEnv(string(raw))
	cnf := &Config{}
	err = yaml.Unmarshal([]byte(parsed), &cnf)
	if nil != err {
		return nil, err
	}
	
	return cnf, nil
}

type (
	Config struct {
		Connection   ConnectionConfig `yaml:"connection"`
		BinLogReader struct {
			Tables   map[string][]string `yaml:"tables"`
			Position struct {
				Directory string `yaml:"directory"`
				Name      string `yaml:"name"`
			} `yaml:"position"`
		} `yaml:"binlogReader"`
	}
	
	ConnectionConfig struct {
		MySQL struct {
			Address    string `yaml:"address"`
			User       string `yaml:"user"`
			Password   string `yaml:"password"`
			EnabledSSL bool   `yaml:"enabledSSL"`
			FromBegin  bool   `yaml:"fromBegin"`
		} `yaml:"mysql"`
		
		EventHub struct {
			ConnectionUrl string           `yaml:"connectionUrl"`
			Publishing    PublishingConfig `yaml:"publishing"`
		} `yaml:"eventhub"`
	}
	
	PublishingConfig struct {
		MaxEvents int           `yaml:"maxEvents"`
		Interval  time.Duration `yaml:"interval"`
	}
)
