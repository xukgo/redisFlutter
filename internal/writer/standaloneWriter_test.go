package writer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"redisFlutter/internal/aof"
	"redisFlutter/internal/config"
	"redisFlutter/internal/entry"
	"redisFlutter/logUtil"
	"testing"
	"time"
)

func Test_standaloneWriter01(t *testing.T) {
	logUtil.InitTestLog()
	fp := "/home/hermes/work/github/redisFlutter/data/b01.aof"
	r, err := os.OpenFile(fp, os.O_RDONLY, 0644)
	if err != nil {
		t.FailNow()
	}

	parser := aof.NewAofStreamParser(r)

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
	redisWriter, err := NewStandaloneWriter(context.Background(), &opts)
	if err != nil {
		t.FailNow()
	}
	redisWriter.StartWrite(context.Background())

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
