package main

import (
	"fmt"
	"log/slog"
	"os"
	"redisFlutter/internal/log"
	"time"
)

func main() {
	fmt.Println("hello world")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // 设置最低日志级别（Debug 及以上会输出）
	}))
	slog.SetDefault(logger)

	log.Init("debug", "0.log", "log", true, 512, 7, 3, true)

	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()
	//
	//opt := reader.SyncReaderOptions{
	//	Address:     "127.0.0.1:36001",
	//	DataDirPath: "/tmp/redisFlutter/inst1",
	//}

	//aofSaveDir := "/tmp/redisFlutter/aof"
	//aofWriter := rotate.NewAOFWriter(opt.Address, aofSaveDir, 0)
	//defer aofWriter.Close()
	//aofSaver, err := aofStorage.NewBadgerAofStorage(aofSaveDir)
	//if err != nil {
	//	panic(err)
	//}
	//r := reader.NewStandaloneReader(ctx, &opt)
	//r.StartRead(ctx)

	for {
		time.Sleep(time.Hour)
	}
}
