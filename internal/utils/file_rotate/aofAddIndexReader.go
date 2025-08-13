package rotate

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/atomic"
	"io"
	"log/slog"
	"os"
	"path"
	"redisFlutter/constDefine"
	"redisFlutter/internal/utils"
	"time"
)

type AofAddIndexReader struct {
	ctx  context.Context
	name string
	dir  string

	file         *os.File
	filepath     string
	fileIndex    int64
	nextFilePath string

	fileReadSize  int64
	totalReadSize *atomic.Int64
}

func NewAofAddIndexReader(ctx context.Context, name string, dir string, startFileIndex int64) *AofAddIndexReader {
	r := new(AofAddIndexReader)
	r.ctx = ctx
	r.name = name
	r.dir = dir
	r.file = nil
	r.fileIndex = startFileIndex
	r.fileReadSize = 0
	r.totalReadSize = atomic.NewInt64(0)
	return r
}

func (c *AofAddIndexReader) getIndexFilePath(index int64) string {
	filepath := path.Join(c.dir, fmt.Sprintf("%d%s", index, constDefine.REDIS_APPEND_CMD_FILE_SUFFIX))
	return filepath
}

func (c *AofAddIndexReader) GetTotalReadSize() int64 {
	return c.totalReadSize.Load()
}

func (c *AofAddIndexReader) openFile(index int64) error {
	c.filepath = c.getIndexFilePath(index)
	c.nextFilePath = c.getIndexFilePath(index + 1)

	var err error
	c.file, err = os.OpenFile(c.filepath, os.O_RDONLY, 0644)
	if err != nil {
		slog.Error("open file for read error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return err
	}
	c.fileReadSize = 0
	slog.Info("open file for read success", slog.String("name", c.name), slog.String("filepath", c.filepath))
	return nil
}

func (c *AofAddIndexReader) justRead(buf []byte) (int, error) {
	n, err := c.file.Read(buf)
	if err == nil {
		c.totalReadSize.Add(int64(n))
		c.fileReadSize += int64(n)
		//slog.Debug("read file success", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int("len", n))
		return n, nil
	}

	return n, err
}

func (c *AofAddIndexReader) Read(buf []byte) (int, error) {
	if c.totalReadSize.Load() == 0 {
		var exist = c.waitFileExist(c.getIndexFilePath(c.fileIndex))
		if !exist {
			return 0, fmt.Errorf("read file not exist after wait time")
		}
		err := c.openFile(c.fileIndex)
		if err != nil {
			return 0, err
		}
	}
	if c.file == nil {
		slog.Error("read file handler is nil", slog.String("name", c.name), slog.String("filepath", c.filepath))
		return 0, fmt.Errorf("read file handler is nil")
	}

	n, err := c.justRead(buf)
	if err == nil {
		return n, nil
	}
	for !errors.Is(err, io.EOF) {
		slog.Error("read file error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return 0, err
	}

	ret, err := c.waitFileExpandOrNextFileExist()
	if err != nil {
		return 0, err
	}
	if ret == 0 {
		return 0, fmt.Errorf("receive context done flag")
	}
	if ret == 1 || ret == 3 { //can read continue
		return c.justRead(buf)
	}

	_ = c.closeCurrentFile()
	return c.readNextFile(buf)
}

func (c *AofAddIndexReader) readNextFile(buf []byte) (int, error) {
	c.fileIndex++
	err := c.openFile(c.fileIndex)
	if err != nil {
		return 0, err
	}

	n, err := c.justRead(buf)
	if err == nil {
		return n, nil
	}
	for !errors.Is(err, io.EOF) {
		slog.Error("read file error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return 0, err
	}

	err = c.waitFileExpand()
	if err != nil {
		return 0, err
	}
	return c.justRead(buf)
}

func (c *AofAddIndexReader) waitFileExpandOrNextFileExist() (int, error) {
	//file.Seek(0, io.SeekEnd)
	checkInterval := time.Millisecond * 100
	ticker := time.NewTicker(checkInterval)
	ticker.Reset(time.Nanosecond)

	startAt := time.Now()
	slog.Info("start wait file expand or next file exist", slog.String("name", c.name), slog.String("filepath", c.filepath))

	for {
		select {
		case <-c.ctx.Done():
			slog.Warn("receive context done flag, wait file expand or next file exist end", slog.String("name", c.name), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.String("filepath", c.filepath))
			return 0, nil
		case <-ticker.C:
			ticker.Reset(checkInterval)
			//must detect exist first
			nextExist := utils.IsExist(c.nextFilePath)
			if nextExist {
				slog.Info("detect next file exist", slog.String("name", c.name), slog.String("nextFilePath", c.nextFilePath))
			}
			//then check file expand
			currentFileSize, err := c.file.Seek(0, io.SeekEnd)
			if err != nil {
				slog.Error("file seek end error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()))
				return 0, err
			}
			if currentFileSize < c.fileReadSize {
				slog.Error("seek file size less than file read size", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.Int64("seekSize", currentFileSize), slog.Int64("fileReadSize", c.fileReadSize))
				return 0, fmt.Errorf("seek file size less than file read size")
			}
			//reset file seek for read
			_, err = c.file.Seek(c.fileReadSize, io.SeekStart)
			if err != nil {
				slog.Error("file seek read error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()))
				return 0, err
			}
			if currentFileSize == c.fileReadSize && !nextExist { // no change
				continue
			}
			if currentFileSize > c.fileReadSize {
				slog.Debug("detect file expand", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.Int64("seekSize", currentFileSize), slog.Int64("fileReadSize", c.fileReadSize))
				if nextExist {
					return 3, nil
				}
				return 1, nil
			}
			if nextExist {
				return 2, nil
			}
		}
	}
}

func (c *AofAddIndexReader) waitFileExpand() error {
	checkInterval := time.Millisecond * 100
	ticker := time.NewTicker(checkInterval)
	ticker.Reset(time.Nanosecond)

	startAt := time.Now()
	slog.Info("start wait file expand", slog.String("name", c.name), slog.String("filepath", c.filepath))

	for {
		select {
		case <-c.ctx.Done():
			slog.Warn("receive context done flag, wait file expand end", slog.String("name", c.name), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.String("filepath", c.filepath))
			return fmt.Errorf("receive context done flag")
		case <-ticker.C:
			ticker.Reset(checkInterval)
			//check file expand
			currentFileSize, err := c.file.Seek(0, io.SeekEnd)
			if err != nil {
				slog.Error("file seek end error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()))
				return err
			}
			if currentFileSize < c.fileReadSize {
				slog.Error("seek file size less than file read size", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.Int64("seekSize", currentFileSize), slog.Int64("fileReadSize", c.fileReadSize))
				return fmt.Errorf("seek file size less than file read size")
			}
			//reset file seek for read
			_, err = c.file.Seek(c.fileReadSize, io.SeekStart)
			if err != nil {
				slog.Error("file seek read error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()))
				return err
			}
			if currentFileSize == c.fileReadSize { // no change
				continue
			}
			slog.Debug("detect file expand", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.Int64("seekSize", currentFileSize), slog.Int64("fileReadSize", c.fileReadSize))
			return nil
		}
	}
}

func (c *AofAddIndexReader) waitFileExist(filePath string) bool {
	exist := utils.IsExist(filePath)
	if exist {
		slog.Info("detect file exist success, wait file exist end", slog.String("name", c.name), slog.String("filepath", filePath))
		return true
	}

	checkInterval := time.Millisecond * 100
	ticker := time.NewTicker(checkInterval)
	ticker.Reset(checkInterval)

	startAt := time.Now()
	slog.Info("start wait file exist", slog.String("name", c.name), slog.String("filepath", filePath))

	for {
		select {
		case <-c.ctx.Done():
			slog.Warn("receive context done flag, wait file exist end", slog.String("name", c.name), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.String("filepath", filePath))
			return false
		case <-ticker.C:
			ticker.Reset(checkInterval)
			exist := utils.IsExist(filePath)
			if exist {
				slog.Info("detect file exist success, wait file exist end", slog.String("name", c.name), slog.Int64("elapsedMs", time.Since(startAt).Milliseconds()), slog.String("filepath", filePath))
				return true
			}
		}
	}
}

func (c *AofAddIndexReader) closeCurrentFile() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	if err != nil {
		slog.Error("close file error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return err
	}
	slog.Info("close file success", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("fileReadSize", c.fileReadSize), slog.Int64("totalReadSize", c.totalReadSize.Load()))

	c.file = nil
	c.fileReadSize = 0
	return nil
}
