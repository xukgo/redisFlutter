package writer

import (
	"fmt"
	"io"
	"os"
	"redisFlutter/internal/aof"
	"redisFlutter/internal/entry"
	"testing"
)

func Test_standaloneWriter01(t *testing.T) {
	fp := "/home/hermes/work/github/redisFlutter/data/b01.aof"
	r, err := os.OpenFile(fp, os.O_RDONLY, 0644)
	if err != nil {
		t.FailNow()
	}

	target := aof.NewAofStreamParser(r)

	count := 0
	e := new(entry.Entry)
	for {
		err = target.ParseNext(e)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.FailNow()
		}
		count++
	}
	fmt.Printf("entry total count: %d\n", count)

}
