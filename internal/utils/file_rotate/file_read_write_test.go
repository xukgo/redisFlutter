package rotate

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"io"
	"os"
	"testing"
	"time"
)

func Test_fileReadWrite01(t *testing.T) {
	fp := fmt.Sprintf("/tmp/%s.tmp", uuid.NewV1().String())
	defer os.Remove(fp)
	writer, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		t.FailNow()
	}
	go func() {
		reader, err := os.OpenFile(fp, os.O_RDONLY, 0644)
		if err != nil {
			t.FailNow()
		}

		var readTotal int64 = 0
		readBuffer := make([]byte, 1024)
		for {
			readCount, err := reader.Read(readBuffer)
			if readCount > 0 {
				readTotal += int64(readCount)
				fmt.Println("read count:", readCount)
				continue
			}
			if err != nil {
				if err != io.EOF {
					fmt.Println("read error:", err)
					break
				}
				time.Sleep(time.Millisecond * 600)
				currentFileSize, _ := reader.Seek(0, io.SeekEnd)
				if currentFileSize > readTotal {
					reader.Seek(readTotal, io.SeekStart)
					continue
				}
				break
			}
		}
	}()

	for i := 0; i < 10; i++ {
		wn, err := writer.Write([]byte(uuid.NewV1().String()))
		if err != nil {
			fmt.Println("write error:", err)
			break
		}
		fmt.Println("write count:", wn)
		time.Sleep(time.Millisecond * 500)
	}
	time.Sleep(time.Second * 2)
}
