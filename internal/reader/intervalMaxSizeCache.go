package reader

import (
	"log/slog"
	"time"
)

type IntervalMaxSizeCache struct {
	interval time.Duration
	startAt  time.Time
	cache    []byte
}

func NewIntervalMaxSizeCache(interval time.Duration, cacheSize int) *IntervalMaxSizeCache {
	return &IntervalMaxSizeCache{
		interval: interval,
		cache:    make([]byte, 0, cacheSize),
		startAt:  time.Now(),
	}
}

func (c *IntervalMaxSizeCache) reset(dt time.Time) {
	c.startAt = dt
	c.cache = c.cache[:0]
	slog.Debug("IntervalMaxSizeCache reset")
}
func (c *IntervalMaxSizeCache) getCache() []byte {
	return c.cache
}
func (c *IntervalMaxSizeCache) append(data []byte) {
	c.cache = append(c.cache, data...)
}
func (c *IntervalMaxSizeCache) getCacheCap() int {
	return cap(c.cache)
}

//	func (c *IntervalMaxSizeCache) CheckAppendEnable(size int) bool {
//		if len(c.cache)+size <= c.getCacheCap() {
//			return true
//		}
//		return false
//	}
func (c *IntervalMaxSizeCache) ActionIfTimeout(dt time.Time, bufferAction func([]byte) error) error {
	cache := c.getCache()
	timeout := c.checkTimeout(dt)
	slog.Debug("IntervalMaxSizeCache checkTimeout", slog.Bool("timeout", timeout), slog.Int("cacheSize", len(cache)))
	if timeout && len(cache) > 0 {
		err := bufferAction(cache)
		c.reset(dt)
		return err
	}
	return nil
}
func (c *IntervalMaxSizeCache) checkTimeout(dt time.Time) bool {
	return dt.Sub(c.startAt).Nanoseconds() > c.interval.Nanoseconds()
}

func (r *IntervalMaxSizeCache) AppendWithAction(dtNow time.Time, buff []byte, bufferAction func([]byte) error) error {
	if len(buff) == 0 {
		return nil
	}

	var err error
	cache := r.getCache()
	if len(buff) > r.getCacheCap() {
		if len(cache) > 0 {
			err = bufferAction(cache)
			//err = r.aofStorage.Append(r.nextKey(), cache)
			r.reset(dtNow)
			if err != nil {
				return err
			}
		}
		err = bufferAction(buff)
		//err = r.aofStorage.Append(r.nextKey(), buff)
		r.reset(dtNow)
		return err
	}

	if len(buff)+len(cache) > r.getCacheCap() {
		if len(cache) > 0 {
			err = bufferAction(cache)
			//err = r.aofStorage.Append(r.nextKey(), cache)
			r.reset(dtNow)
			if err != nil {
				return err
			}
		}
		r.append(buff)
		return nil
	}

	if len(buff)+len(cache) == r.getCacheCap() {
		r.append(buff)
		err = bufferAction(r.getCache())
		//err = r.aofStorage.Append(r.nextKey(), r.getCache())
		r.reset(dtNow)
		return err
	}

	r.append(buff)
	return nil
}
