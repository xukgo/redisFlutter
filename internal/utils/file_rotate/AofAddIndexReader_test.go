package rotate

import (
	"context"
	"github.com/stretchr/testify/assert"
	"redisFlutter/logUtil"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test_AofAddIndexReader_wait_first_file(t *testing.T) {
	logUtil.InitTestLog()
	dir := "/tmp/gtest/"
	ctx0, cancel0 := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(2 * time.Second)
		cancel0()
		wg.Done()
	}()

	reader := NewAofAddIndexReader(ctx0, "testReader", dir, 0)
	wbuff := make([]byte, 512)
	for {
		_, err := reader.Read(wbuff)
		if err != nil {
			if strings.Contains(err.Error(), "wait") {
				break
			}
			t.Error(err)
			t.FailNow()
		}
	}
	wg.Wait()
	time.Sleep(time.Second)
}

func Test_AofAddIndexReader_wait_write_empty(t *testing.T) {
	logUtil.InitTestLog()
	dir := "/tmp/gtest/"
	writer, err := NewAofAddIndexWriter("testWriter", dir, 1024*4)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer writer.RemoveAll()

	//wtotal := int64(0)
	//wbuff := make([]byte, 500)
	//for i := 0; i < 100; i++ {
	//	writer.Write(wbuff)
	//	wtotal += int64(len(wbuff))
	//}
	//writer.Close()

	ctx0, cancel0 := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(2 * time.Second)
		cancel0()
		wg.Done()
	}()

	reader := NewAofAddIndexReader(ctx0, "testReader", dir, 0)
	rbuff := make([]byte, 512)
	for {
		_, err := reader.Read(rbuff)
		if err != nil {
			if strings.Contains(err.Error(), "context") {
				break
			}
			t.Error(err)
			t.FailNow()
		}
	}
	wg.Wait()
	time.Sleep(time.Second)
}

func Test_AofAddIndexReader_wait_write_little(t *testing.T) {
	logUtil.InitTestLog()
	dir := "/tmp/gtest/"
	writer, err := NewAofAddIndexWriter("testWriter", dir, 1024)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer writer.RemoveAll()

	wtotal := int64(0)
	wbuff := make([]byte, 512)
	go func() {
		time.Sleep(1 * time.Second)
		writer.Write(wbuff)
		wtotal += int64(len(wbuff))
		writer.Close()
	}()
	//for i := 0; i < 100; i++ {
	//	writer.Write(wbuff)
	//	wtotal += int64(len(wbuff))
	//}
	//writer.Close()

	ctx0, cancel0 := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(2 * time.Second)
		cancel0()
		wg.Done()
	}()

	rtotal := int64(0)
	reader := NewAofAddIndexReader(ctx0, "testReader", dir, 0)
	rbuff := make([]byte, 512)
	for {
		n, err := reader.Read(rbuff)
		if err != nil {
			if strings.Contains(err.Error(), "context") {
				break
			}
			t.Error(err)
			t.FailNow()
		}
		rtotal += int64(n)
	}
	wg.Wait()
	time.Sleep(time.Second)
	assert.Equal(t, reader.GetTotalReadSize(), int64(wtotal))
	assert.Equal(t, reader.GetTotalReadSize(), int64(rtotal))
}

func Test_AofAddIndexReader_wait_write_01(t *testing.T) {
	logUtil.InitTestLog()
	dir := "/tmp/gtest/"
	writer, err := NewAofAddIndexWriter("testWriter", dir, 1024)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer writer.RemoveAll()

	wtotal := int64(0)
	wbuff := make([]byte, 512)
	go func() {
		time.Sleep(1 * time.Second)
		for i := 0; i < 2; i++ {
			writer.Write(wbuff)
			wtotal += int64(len(wbuff))
		}
		writer.Close()
	}()

	ctx0, cancel0 := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(6 * time.Second)
		cancel0()
		wg.Done()
	}()

	rtotal := int64(0)
	reader := NewAofAddIndexReader(ctx0, "testReader", dir, 0)
	rbuff := make([]byte, 512)
	for {
		n, err := reader.Read(rbuff)
		if err != nil {
			if strings.Contains(err.Error(), "context") {
				break
			}
			t.Error(err)
			t.FailNow()
		}
		rtotal += int64(n)
	}
	wg.Wait()
	time.Sleep(time.Second)
	assert.Equal(t, reader.GetTotalReadSize(), int64(wtotal))
	assert.Equal(t, reader.GetTotalReadSize(), int64(rtotal))
}

func Test_AofAddIndexReader_wait_write_02(t *testing.T) {
	logUtil.InitTestLog()
	dir := "/tmp/gtest/"
	writer, err := NewAofAddIndexWriter("testWriter", dir, 1024)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer writer.RemoveAll()

	wtotal := int64(0)
	wbuff := make([]byte, 512)
	go func() {
		time.Sleep(1 * time.Second)
		for i := 0; i < 2; i++ {
			writer.Write(wbuff)
			wtotal += int64(len(wbuff))
		}
		time.Sleep(2 * time.Second)
		for i := 0; i < 2; i++ {
			writer.Write(wbuff)
			wtotal += int64(len(wbuff))
		}
		writer.Close()
	}()

	ctx0, cancel0 := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(6 * time.Second)
		cancel0()
		wg.Done()
	}()

	rtotal := int64(0)
	reader := NewAofAddIndexReader(ctx0, "testReader", dir, 0)
	rbuff := make([]byte, 200)
	for {
		n, err := reader.Read(rbuff)
		if err != nil {
			if strings.Contains(err.Error(), "context") {
				break
			}
			t.Error(err)
			t.FailNow()
		}
		rtotal += int64(n)
	}
	wg.Wait()
	time.Sleep(time.Second)
	assert.Equal(t, reader.GetTotalReadSize(), int64(wtotal))
	assert.Equal(t, reader.GetTotalReadSize(), int64(rtotal))
}

func Test_AofAddIndexReader_trace_read_fuzz(t *testing.T) {
	logUtil.InitTestLog()
	dir := "/tmp/gtest/"
	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx0, cancel0 := context.WithCancel(context.Background())

	writer, err := NewAofAddIndexWriter("testWriter", dir, 200)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer writer.RemoveAll()

	wtotal := int64(0)
	wbuff := make([]byte, 30)
	go func() {
		time.Sleep(1 * time.Second)
		for i := 0; i < 2000; i++ {
			n, _ := writer.Write(wbuff)
			wtotal += int64(n)
			time.Sleep(1 * time.Millisecond)
		}
		writer.Close()
		go func() {
			time.Sleep(5 * time.Second)
			cancel0()
			wg.Done()
		}()
	}()

	rtotal := int64(0)
	reader := NewAofAddIndexReader(ctx0, "testReader", dir, 0)
	rbuff := make([]byte, 15)
	for {
		n, err := reader.Read(rbuff)
		if err != nil {
			if strings.Contains(err.Error(), "context") {
				break
			}
			t.Error(err)
			t.FailNow()
		}
		rtotal += int64(n)
	}
	assert.Equal(t, reader.GetTotalReadSize(), int64(wtotal))
	assert.Equal(t, reader.GetTotalReadSize(), int64(rtotal))
	wg.Wait()
	time.Sleep(time.Second)
	assert.Equal(t, reader.GetTotalReadSize(), int64(wtotal))
	assert.Equal(t, reader.GetTotalReadSize(), int64(rtotal))
}
