package rotate

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
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
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".aof") {
			info, _ := entry.Info()
			totalSize += info.Size()
		}
	}
	assert.Equal(t, wtotal, totalSize)
}
