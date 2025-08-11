package rdb

import (
	"context"
	"fmt"
	"redisFlutter/internal/entry"
	"testing"
	"time"
)

func Test_rdb_load01(t *testing.T) {
	rdbFilePath := "/home/hermes/work/github/redisFlutter/data/dump.rdb"
	//updateFunc := func(offset int64) {
	//	fmt.Printf("update offset=%d\n", offset)
	//}
	ch := make(chan *entry.Entry, 40960)
	redisCmdDict := make(map[string]int64, 32)
	go func() {
		for e := range ch {
			_ = e
			if len(e.Argv) > 0 {
				k1 := e.Argv[0]
				if _, ok := redisCmdDict[k1]; !ok {
					redisCmdDict[k1] = 0
				}
				redisCmdDict[k1]++
			}
			//fmt.Println(e.String())
		}
	}()
	rdbLoader := NewLoader("testRdb", rdbFilePath)
	rdbLoader.SetEntryCallback(func(e *entry.Entry) {
		ch <- e
	})
	dbId := rdbLoader.ParseRDB(context.Background())
	fmt.Printf("dbId=%d\n", dbId)
	fmt.Printf("rdbSize=%d\n", rdbLoader.GetRdbSize()) //rdbSize=15180508
	time.Sleep(time.Second * 6)
	fmt.Printf("cmd dict:%v\n", redisCmdDict) //cmd dict:map[del:4248 hset:55815 sadd:26480 set:180 zadd:325167]
}
