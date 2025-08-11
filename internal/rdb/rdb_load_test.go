package rdb

import (
	"context"
	"fmt"
	"redisFlutter/internal/entry"
	"testing"
)

func Test_rdb_load01(t *testing.T) {
	rdbFilePath := "/home/hermes/work/github/redisFlutter/data/dump.rdb"
	//updateFunc := func(offset int64) {
	//	fmt.Printf("update offset=%d\n", offset)
	//}
	ch := make(chan *entry.Entry, 40960)
	go func() {
		for e := range ch {
			_ = e
			//fmt.Println(e.String())
		}
	}()
	rdbLoader := NewLoader("testRdb", rdbFilePath, ch)
	dbId := rdbLoader.ParseRDB(context.Background())
	fmt.Printf("dbId=%d\n", dbId)
	fmt.Printf("rdbSize=%d\n", rdbLoader.GetRdbSize())
}
