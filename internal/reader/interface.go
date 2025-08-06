package reader

import (
	"context"
	"redisFlutter/internal/entry"
	"redisFlutter/internal/status"
)

type Reader interface {
	status.Statusable
	StartRead(ctx context.Context) []chan *entry.Entry
}
