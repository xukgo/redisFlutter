package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"redisFlutter/internal/aofStorage"
	"redisFlutter/internal/log"
	"redisFlutter/internal/reader"
	"time"
)

func main() {
	fmt.Println("hello world")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // 设置最低日志级别（Debug 及以上会输出）
	}))
	slog.SetDefault(logger)
	
	log.Init("debug", "0.log", "log", true, 512, 7, 3, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := reader.SyncReaderOptions{
		Address:     "192.168.157.224:36001",
		DataDirPath: "/tmp/redisFlutter",
	}

	aofSaver, err := aofStorage.NewBadgerAofStorage("/tmp/redisFlutter/aof")
	if err != nil {
		panic(err)
	}
	r := reader.NewStandaloneReader(ctx, &opt, aofSaver)
	r.StartRead(ctx)

	for {
		time.Sleep(time.Hour)
	}
}
