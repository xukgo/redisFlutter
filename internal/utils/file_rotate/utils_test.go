package rotate

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"redisFlutter/constDefine"
	"testing"
)

func Test_ScanAddIndexSuffixFiles(t *testing.T) {
	dirPath := path.Join("/tmp", uuid.NewV1().String())
	os.MkdirAll(dirPath, 0755)
	defer os.RemoveAll(dirPath)
	for i := 0; i < 100; i += 2 {
		fname := fmt.Sprintf("%d.aof", i)
		fpath := path.Join(dirPath, fname)
		f, err := os.Create(fpath)
		if err != nil {
			t.Fatalf("failed to create file %s: %s", fpath, err)
			return
		}
		f.Close()
	}
	for i := 0; i < 20; i += 1 {
		fname := fmt.Sprintf("a%d.aof", i)
		fpath := path.Join(dirPath, fname)
		f, err := os.Create(fpath)
		if err != nil {
			t.Fatalf("failed to create file %s: %s", fpath, err)
			return
		}
		f.Close()
	}
	for i := 0; i < 20; i += 1 {
		fname := fmt.Sprintf("%da.aof", i)
		fpath := path.Join(dirPath, fname)
		f, err := os.Create(fpath)
		if err != nil {
			t.Fatalf("failed to create file %s: %s", fpath, err)
			return
		}
		f.Close()
	}
	arr := ScanAddIndexSuffixFiles(dirPath, constDefine.REDIS_APPEND_CMD_FILE_SUFFIX)
	assert.True(t, len(arr) == 50)
}
