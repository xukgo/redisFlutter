package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger zerolog.Logger

func Init(level string, file string, dir string, rotation bool, size int, age int, backups int, compress bool) {
	// log level
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	default:
		panic(fmt.Sprintf("unknown log level: %s", level))
	}

	// dir
	dir, err := filepath.Abs(dir)
	if err != nil {
		panic(fmt.Sprintf("failed to determine current directory: %v", err))
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0777)
		if err != nil {
			panic(fmt.Sprintf("mkdir failed. dir=[%s], error=[%v]", dir, err))
		}
	}
	path := filepath.Join(dir, file)

	// log file
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	var fileWriter io.Writer
	if rotation {
		fileWriter = &lumberjack.Logger{
			Filename:   path,
			MaxSize:    size, // megabytes
			MaxBackups: backups,
			MaxAge:     age,      //days
			Compress:   compress, // disabled by default
			LocalTime:  true,
		}
	} else {
		fileWriter, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	}
	if err != nil {
		panic(fmt.Sprintf("open log file failed. file=[%s], err=[%s]", path, err))
	}
	multi := zerolog.MultiLevelWriter(consoleWriter, fileWriter)
	logger = zerolog.New(multi).With().Timestamp().Logger()
	Infof("log_level: [%v], log_file: [%v]", level, path)
}
