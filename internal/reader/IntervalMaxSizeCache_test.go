package reader

import (
	"testing"
	"time"
)

func Test_IntervalMaxSizeCache01(t *testing.T) {
	target := NewIntervalMaxSizeCache(time.Millisecond*500, 16*1024)

	target.AppendWithAction()
}
