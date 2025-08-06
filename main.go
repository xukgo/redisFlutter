package main

import (
	"context"
	"fmt"
	"redisFlutter/internal/log"
	"redisFlutter/internal/reader"
	"time"
)

func main() {
	fmt.Println("hello world")

	log.Init("debug", "0.log", "data", true, 512, 7, 3, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := reader.SyncReaderOptions{
		Address: "192.168.157.224:36001",
	}
	r := reader.NewStandaloneReader(ctx, &opt)
	r.StartRead(ctx)

	for {
		time.Sleep(time.Hour)
	}
}
