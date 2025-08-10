package logUtil

import (
	"log/slog"
	"os"
)

func InitLog() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // 设置最低日志级别（Debug及以上会输出）
	})

	slog.SetDefault(slog.New(handler))
}

func InitTestLog() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // 设置最低日志级别（Debug及以上会输出）
	})

	slog.SetDefault(slog.New(handler))
}
