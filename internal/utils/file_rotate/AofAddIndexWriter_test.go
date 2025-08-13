package rotate

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"redisFlutter/constDefine"
	"strings"
	"testing"
)

func Test_AofAddIndexWriter01(t *testing.T) {
	dir := "/tmp/gtest/"
	target, err := NewAofAddIndexWriter("testAofAddIndexWriter", "/tmp/gtest", 1024*4)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer target.RemoveAll()

	wtotal := int64(0)
	wbuff := make([]byte, 500)
	for i := 0; i < 100; i++ {
		target.Write(wbuff)
		wtotal += int64(len(wbuff))
	}
	target.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		t.Error(err)
		t.FailNow()
	}
	totalSize := int64(0)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), constDefine.REDIS_APPEND_CMD_FILE_SUFFIX) {
			info, _ := entry.Info()
			totalSize += info.Size()
		}
	}
	assert.Equal(t, wtotal, totalSize)
}

func Test_AofAddIndexWriter02(t *testing.T) {
	dir := "/tmp/gtest/"
	target, err := NewAofAddIndexWriter("testAofAddIndexWriter02", dir, 5000)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer target.RemoveAll()

	wtotal := int64(0)
	wbuff := make([]byte, 500)
	for i := 0; i < 99; i++ {
		target.Write(wbuff)
		wtotal += int64(len(wbuff))
	}
	target.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		t.Error(err)
		t.FailNow()
	}
	totalSize := int64(0)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), constDefine.REDIS_APPEND_CMD_FILE_SUFFIX) {
			info, _ := entry.Info()
			totalSize += info.Size()
		}
	}
	assert.Equal(t, wtotal, totalSize)

	target2, err := NewAofAddIndexWriter("testAofAddIndexWriter02", dir, 5000)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	assert.True(t, target2.fileIndex == 9)
	target2.Close()

	target3, err := NewAofAddIndexWriter("testAofAddIndexWriter02", dir, 3000)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	assert.True(t, target3.fileIndex == 10)
	target3.Close()
}
