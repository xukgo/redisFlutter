package reader

import (
	"context"
	"testing"
)

func Test_aof_reader01(t *testing.T) {
	aofFilePath := "/home/hermes/work/github/redisFlutter/data/b01.aof"
	r := NewAOFReader(&AOFReaderOptions{
		Filepath:     aofFilePath,
		AOFTimestamp: 0,
	})
	ch := r.StartRead(context.Background())
	for e := range ch[0] {
		println(e.String())
	}
}
