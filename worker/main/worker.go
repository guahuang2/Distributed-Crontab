package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/owenliang/crontab/worker"
)

var (
	confFile string
)

func initArgs() {
	// worker -config ./worker.json
	// worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	initArgs()

	initEnv()

	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	if err = worker.InitRegister(); err != nil {
		goto ERR
	}

	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}

	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	return

ERR:
	fmt.Println(err)
}
