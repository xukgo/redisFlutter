package reader

import "time"

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

func (c *IntervalMaxSizeCache) Reset(dt time.Time) {
	c.startAt = dt
	c.cache = c.cache[:0]
}
func (c *IntervalMaxSizeCache) GetCache() []byte {
	return c.cache
}
func (c *IntervalMaxSizeCache) Append(data []byte) {
	c.cache = append(c.cache, data...)
}
func (c *IntervalMaxSizeCache) GetCacheCap() int {
	return cap(c.cache)
}
func (c *IntervalMaxSizeCache) CheckAppendEnable(size int) bool {
	if len(c.cache)+size <= c.GetCacheCap() {
		return true
	}
	return false
}
func (c *IntervalMaxSizeCache) CheckTimeout(dt time.Time) bool {
	return dt.Sub(c.startAt).Nanoseconds() > c.interval.Nanoseconds()
}

func (r *IntervalMaxSizeCache) AppendWithAction(dtNow time.Time, buff []byte, bufferAction func([]byte) error) error {
	if len(buff) == 0 {
		return nil
	}

	var err error
	cache := r.GetCache()
	if len(buff) > r.GetCacheCap() {
		if len(cache) > 0 {
			err = bufferAction(cache)
			//err = r.aofStorage.Append(r.nextKey(), cache)
			r.Reset(dtNow)
			if err != nil {
				return err
			}
		}
		err = bufferAction(buff)
		//err = r.aofStorage.Append(r.nextKey(), buff)
		r.Reset(dtNow)
		return err
	}

	if len(buff)+len(cache) > r.GetCacheCap() {
		if len(cache) > 0 {
			err = bufferAction(cache)
			//err = r.aofStorage.Append(r.nextKey(), cache)
			r.Reset(dtNow)
			if err != nil {
				return err
			}
		}
		r.Append(buff)
		return nil
	}

	if len(buff)+len(cache) == r.GetCacheCap() {
		r.Append(buff)
		err = bufferAction(r.GetCache())
		//err = r.aofStorage.Append(r.nextKey(), r.GetCache())
		r.Reset(dtNow)
		return err
	}

	r.Append(buff)
	return nil
}
