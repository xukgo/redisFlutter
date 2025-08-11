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
	redisCmdDict := make(map[string]int64, 32)
	rdbLoader := NewLoader("testRdb", rdbFilePath)
	rdbLoader.SetEntryCallback(func(e *entry.Entry) {
		if len(e.Argv) > 0 {
			k1 := e.Argv[0]
			if _, ok := redisCmdDict[k1]; !ok {
				redisCmdDict[k1] = 0
			}
			redisCmdDict[k1]++
		}
	})
	dbId := rdbLoader.ParseRDB(context.Background())
	fmt.Printf("dbId=%d\n", dbId)
	fmt.Printf("rdbSize=%d\n", rdbLoader.GetRdbSize()) //rdbSize=15180508
	time.Sleep(time.Second * 1)
	fmt.Printf("cmd dict:%v\n", redisCmdDict) //redis cmd dict: map[PEXPIRE:3 XGROUP:1 del:13287 hset:46311 rpush:5 sadd:24740 set:343 xadd:3 xsetid:3 zadd:328905]
}
