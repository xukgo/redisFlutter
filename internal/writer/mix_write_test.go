package writer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"redisFlutter/internal/aof"
	"redisFlutter/internal/config"
	"redisFlutter/internal/entry"
	"redisFlutter/internal/rdb"
	"redisFlutter/logUtil"
	"testing"
	"time"
)

func Test_write_rdb_aof_to_redis(t *testing.T) {
	logUtil.InitTestLog()
	config.Opt.Advanced = config.AdvancedOptions{
		PipelineCountLimit:              1024,
		TargetRedisClientMaxQuerybufLen: 1024000000,
		TargetRedisProtoMaxBulkLen:      512000000,
	}

	opts := RedisWriterOptions{
		Cluster:  false,
		Address:  "127.0.0.1:36002",
		OffReply: false,
	}
	redisWriter := NewStandaloneWriter(context.Background(), &opts)
	redisWriter.StartWrite(context.Background())

	//rdb
	rdbFilePath := "/home/hermes/work/github/redisFlutter/data/dump.rdb"
	redisCmdDict := make(map[string]int64, 32)
	rdbLoader := rdb.NewLoader("testRdb", rdbFilePath)
	rdbLoader.SetEntryCallback(func(e *entry.Entry) {
		//slog.Debug("rdb send cmd", slog.String("cmd", e.Argv[0]))
		redisWriter.Write(e.Clone())
		if len(e.Argv) > 0 {
			k1 := e.Argv[0]
			if _, ok := redisCmdDict[k1]; !ok {
				redisCmdDict[k1] = 0
			}
			redisCmdDict[k1]++
		}
	})
	rdbLoader.ParseRDB(context.Background())

	//aof
	aofFilePath := "/home/hermes/work/github/redisFlutter/data/b01.aof"
	r, err := os.OpenFile(aofFilePath, os.O_RDONLY, 0644)
	if err != nil {
		t.FailNow()
	}

	parser := aof.NewAofStreamParser(r)

	count := 0
	e := new(entry.Entry)
	for {
		err = parser.ParseNext(e)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.FailNow()
		}
		redisWriter.Write(e.Clone())
		count++
	}
	fmt.Printf("entry total count: %d\n", count)
	time.Sleep(3 * time.Second)
	redisWriter.Close()
}
