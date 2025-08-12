package main

import (
	"redisFlutter/logUtil"
	"time"
)

func main() {
	logUtil.InitLog()
	addr := ":8061"
	server := NewFileUploadServer(addr)
	go server.Start()

	for {
		time.Sleep(time.Hour)
	}
}
