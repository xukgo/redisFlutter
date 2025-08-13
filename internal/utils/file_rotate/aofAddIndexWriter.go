package rotate

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"redisFlutter/constDefine"
	"strings"
)

//const MaxFileSize = 8 * 1024 * 1024 // 8M

type AofAddIndexWriter struct {
	name string
	dir  string

	singleFileMaxSize int64
	file              *os.File
	filepath          string
	fileIndex         int64

	filesize int64
}

func NewAofAddIndexWriter(name string, dir string, singleFileMaxSize int64) (*AofAddIndexWriter, error) {
	w := new(AofAddIndexWriter)
	w.name = name
	w.dir = dir
	w.singleFileMaxSize = singleFileMaxSize
	os.MkdirAll(dir, 0755)

	indexArr := ScanAddIndexSuffixFiles(dir, constDefine.REDIS_APPEND_CMD_FILE_SUFFIX)
	if len(indexArr) == 0 {
		w.fileIndex = 0
		w.file = nil
		w.filesize = 0
		err := w.openNewFile(w.fileIndex)
		return w, err
	} else {
		maxIndex := indexArr[len(indexArr)-1]
		cfpath := path.Join(dir, fmt.Sprintf("%d%s", maxIndex, constDefine.REDIS_APPEND_CMD_FILE_SUFFIX))
		finfo, err := os.Stat(cfpath)
		if err != nil {
			return w, err
		}

		fsize := finfo.Size()
		if fsize >= w.singleFileMaxSize {
			w.fileIndex = maxIndex + 1
			w.file = nil
			w.filesize = 0
			err = w.openNewFile(w.fileIndex)
			return w, err
		}

		w.fileIndex = maxIndex
		w.file = nil
		err = w.openExistFile(cfpath)
		return w, err
	}
}

func (c *AofAddIndexWriter) Reinit() error {
	c.Close()
	c.RemoveAll()

	c.file = nil
	c.filesize = 0
	c.fileIndex = 0
	c.filesize = 0

	err := c.openNewFile(c.fileIndex)
	return err
}

func (c *AofAddIndexWriter) openExistFile(fp string) error {
	c.filepath = fp
	var err error
	c.file, err = os.OpenFile(fp, os.O_WRONLY, 0644) //os.O_TRUNC,os.O_APPEND
	if err != nil {
		slog.Error("open exist file for write error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return err
	}
	c.filesize, _ = c.file.Seek(0, io.SeekEnd)
	c.file.Seek(0, io.SeekStart)
	slog.Info("open exist file for write success", slog.String("name", c.name), slog.String("filepath", c.filepath))
	return nil
}

func (c *AofAddIndexWriter) openNewFile(index int64) error {
	c.filepath = path.Join(c.dir, fmt.Sprintf("%d%s", index, constDefine.REDIS_APPEND_CMD_FILE_SUFFIX))
	var err error
	c.file, err = os.OpenFile(c.filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) //os.O_TRUNC,os.O_APPEND
	if err != nil {
		slog.Error("open new file for write error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return err
	}
	c.filesize = 0
	slog.Info("open new file for write success", slog.String("name", c.name), slog.String("filepath", c.filepath))
	return nil
}

func (c *AofAddIndexWriter) Write(buf []byte) (int, error) {
	n, err := c.file.Write(buf)
	if err != nil {
		slog.Error("write file error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return 0, err
	}

	c.filesize += int64(n)
	//slog.Debug("write file success", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int("len", n))
	if c.filesize >= c.singleFileMaxSize {
		c.Close()
		c.fileIndex++
		err = c.openNewFile(c.fileIndex)
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (c *AofAddIndexWriter) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Sync()
	if err != nil {
		slog.Error("sync file error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return err
	}
	err = c.file.Close()
	if err != nil {
		slog.Error("close file error", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.String("error", err.Error()))
		return err
	}
	c.file = nil
	slog.Info("close file success", slog.String("name", c.name), slog.String("filepath", c.filepath), slog.Int64("filesize", c.filesize))
	return nil
}

func (c *AofAddIndexWriter) RemoveAll() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		slog.Error("read directory error", slog.String("name", c.name), slog.String("dir", c.dir), slog.String("error", err.Error()))
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), constDefine.REDIS_APPEND_CMD_FILE_SUFFIX) {
			fname := entry.Name()
			fullPath := filepath.Join(c.dir, fname)
			err = os.RemoveAll(fullPath)
			if err != nil {
				slog.Error("remove file failed", slog.String("name", c.name), slog.String("filename", fname))
			} else {
				slog.Info("remove file success", slog.String("name", c.name), slog.String("filename", fname))
			}
		}
	}
	return nil
}
