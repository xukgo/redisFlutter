package writer

import (
	"context"
	"redisFlutter/internal/entry"
	"redisFlutter/internal/status"
)

type Writer interface {
	status.Statusable
	Write(entry *entry.Entry)
	StartWrite(ctx context.Context) (ch chan *entry.Entry)
	Close()
}
