package configRepo

import (
	"io"
	"log/slog"
	"os"
)

func InitFromJsonFile(filePath string) error {
	fi, err := os.Open(filePath)
	if err != nil {
		slog.Error("init json file error", slog.String("filePath", filePath), slog.String("err", err.Error()))
		return err
	}
	defer fi.Close()
	return InitFromJsonReader(fi)
}

func InitFromJsonReader(reader io.Reader) error {
	gson, err := io.ReadAll(reader)
	if err != nil {
		slog.Error("json reader error", slog.String("error", err.Error()))
		return err
	}
	GetSingleton().UpdateFromContent(gson)
	return nil
}
