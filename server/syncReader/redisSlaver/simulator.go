package redisSlaver

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"redisFlutter/server/syncReader/configRepo"
	"time"
)

type Simulator struct {
	initRedisCfg *configRepo.RedisServerConfig
}

func NewSimulator(redisCfg *configRepo.RedisServerConfig) *Simulator {
	c := &Simulator{
		initRedisCfg: redisCfg,
	}
	return c
}
func (c *Simulator) Start() {
	c.waitConnectionAllReady()

	//opt := reader.SyncReaderOptions{
	//	Address:     "127.0.0.1:36001",
	//	DataDirPath: "/tmp/redisFlutter/inst1",
	//}
	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()
	//r := reader.NewStandaloneReader(ctx, &opt)
	//r.StartRead(ctx)
}

func (c *Simulator) waitConnectionAllReady() {
	addr := c.initRedisCfg.Addr
	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: c.initRedisCfg.Password, // no password set
		DB:       0,                       // use default DB

		PoolSize:     3,               // max connect count
		MinIdleConns: 2,               //
		DialTimeout:  2 * time.Second, // 连接建立超时
		ReadTimeout:  3 * time.Second, // 读操作超时
		WriteTimeout: 3 * time.Second, // 写操作超时
	})
	defer redisClient.Close()

	for {
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel0()

		startAt := time.Now()
		scmd := redisClient.Ping(ctx0)
		if scmd.Err() != nil {
			slog.Error("redis ping error", slog.String("addr", addr), slog.String("err", scmd.Err().Error()))
			continue
		}
		slog.Info("redis ping success", slog.String("addr", addr), slog.Int64("elapseMs", time.Since(startAt).Milliseconds()))
		break
	}

	slog.Info("waitConnectionAllReady done", slog.String("addr", addr), slog.String("type", "standalone"))
}
