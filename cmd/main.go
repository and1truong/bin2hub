package main

import (
	"context"
	"os"
	
	"github.com/andytruong/bin2hub/pkg"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	
	// Get path to configuration file
	path := os.Getenv("CONFIG")
	if path == "" {
		panic("missing env CONFIG")
	}
	
	logger, err := zap.NewDevelopment()
	if nil != err {
		panic(errors.Wrap(err, "failed to create logger"))
	}
	
	// Parse configuration
	cnf, err := pkg.NewConfig(path)
	if nil != err {
		panic(errors.Wrap(err, "failed to create configuration"))
	}
	
	// Create application
	app, err := pkg.NewApplication(cnf, logger)
	if nil != err {
		panic(errors.Wrap(err, "failed to create application"))
	}
	
	// Run application
	if err := app.Run(ctx); nil != err {
		panic(errors.Wrap(err, "application error"))
	}
}
