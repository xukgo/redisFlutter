package main

import (
	"os"
	"redisFlutter/logUtil"
	"redisFlutter/server/syncReader/configRepo"
	"redisFlutter/server/syncReader/redisSlaver"
	"time"
)

func main() {
	logUtil.InitLog()

	//hostp := flag.String("h", "127.0.0.1", "Redis server IP address")
	//portp := flag.Int("p", 6379, "Redis server port")
	//
	//// 解析命令行参数
	//flag.Parse()
	//
	//// 输出解析结果
	//addr := fmt.Sprintf("%s:%d", *hostp, *portp)

	var err error
	err = configRepo.InitFromJsonFile("/home/hermes/work/github/redisFlutter/server/syncReader/conf/config.json")
	if err != nil {
		os.Exit(-1)
	}

	var configProvider configRepo.IConfigProvider = configRepo.GetSingleton()

	var redisConfig configRepo.RedisServerConfig
	configProvider.ReadCall(func(cfg *configRepo.ServerConfig) {
		redisConfig = cfg.RedisServer
	})
	slaver := redisSlaver.NewSimulator(&redisConfig)
	slaver.Start()

	for {
		time.Sleep(time.Hour)
	}
}
