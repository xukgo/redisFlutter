package writer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"redisFlutter/internal/client"
	"redisFlutter/internal/client/proto"
	"redisFlutter/internal/config"
	"redisFlutter/internal/entry"
	"redisFlutter/internal/log"
)

type RedisWriterOptions struct {
	Cluster   bool             `mapstructure:"cluster" default:"false"`
	Address   string           `mapstructure:"address" default:""`
	Username  string           `mapstructure:"username" default:""`
	Password  string           `mapstructure:"password" default:""`
	Tls       bool             `mapstructure:"tls" default:"false"`
	TlsConfig client.TlsConfig `mapstructure:"tls_config" default:"{}"`
	OffReply  bool             `mapstructure:"off_reply" default:"false"`
}

type StandaloneWriter struct {
	address string
	client  *client.Redis
	DbId    int

	chWaitReply chan *entry.Entry
	chWaitWg    sync.WaitGroup
	offReply    bool
	ch          chan *entry.Entry
	chWg        sync.WaitGroup

	stat struct {
		Name              string `json:"name"`
		UnansweredBytes   int64  `json:"unanswered_bytes"`
		UnansweredEntries int64  `json:"unanswered_entries"`
	}
}

func NewStandaloneWriter(ctx context.Context, opts *RedisWriterOptions) (Writer, error) {
	var err error
	rw := new(StandaloneWriter)
	rw.address = opts.Address
	rw.stat.Name = "writer_" + strings.Replace(opts.Address, ":", "_", -1)
	rw.client, err = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
	if err != nil {
		return nil, err
	}
	rw.ch = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
	if opts.OffReply {
		log.Infof("turn off the reply of write")
		rw.offReply = true
		rw.client.Send("CLIENT", "REPLY", "OFF")
	} else {
		rw.chWaitReply = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit*2)
		rw.chWaitWg.Add(1)
		go rw.processReply()
	}
	return rw, nil
}

func (w *StandaloneWriter) Close() {
	if !w.offReply {
		close(w.ch)
		w.chWg.Wait()
		close(w.chWaitReply)
		w.chWaitWg.Wait()
	}
}

func (w *StandaloneWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	w.chWg = sync.WaitGroup{}
	w.chWg.Add(1)
	go w.processWrite(ctx)
	return w.ch
}

func (w *StandaloneWriter) Write(e *entry.Entry) {
	w.ch <- e
}

func (w *StandaloneWriter) switchDbTo(newDbId int) {
	log.Debugf("[%s] switch db to [%d]", w.stat.Name, newDbId)
	w.client.Send("select", strconv.Itoa(newDbId))
	w.DbId = newDbId
	if !w.offReply {
		w.chWaitReply <- &entry.Entry{
			Argv:    []string{"select", strconv.Itoa(newDbId)},
			CmdName: "select",
		}
	}
}

func (w *StandaloneWriter) processWrite(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// do nothing until w.ch is closed
		case <-ticker.C:
			w.client.Flush()
		case e, ok := <-w.ch:
			if !ok {
				// clean up and exit
				w.client.Flush()
				w.chWg.Done()
				return
			}
			// switch db if we need
			if w.DbId != e.DbId {
				w.switchDbTo(e.DbId)
			}
			// send
			bytes := e.Serialize()
			for e.SerializedSize+atomic.LoadInt64(&w.stat.UnansweredBytes) > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen {
				time.Sleep(1 * time.Nanosecond)
			}
			//slog.Debug("send redis cmd", slog.String("cmd", e.String()))
			if !w.offReply {
				select {
				case w.chWaitReply <- e:
				default:
					w.client.Flush()
					w.chWaitReply <- e
				}
				atomic.AddInt64(&w.stat.UnansweredBytes, e.SerializedSize)
				atomic.AddInt64(&w.stat.UnansweredEntries, 1)
			}
			w.client.SendBytesBuff(bytes)
		}
	}
}

func (w *StandaloneWriter) processReply() {
	var count int64 = 0
	for e := range w.chWaitReply {
		reply, err := w.client.Receive()
		_ = reply
		//slog.Debug("receive redis reply", slog.Any("reply", reply), slog.String("cmd", e.String()))
		count++

		// It's good to skip the nil error since some write commands will return the null reply. For example,
		// the SET command with NX option will return nil if the key already exists.
		if err != nil && !errors.Is(err, proto.Nil) {
			if err.Error() == "BUSYKEY Target key name already exists." {
				if config.Opt.Advanced.RDBRestoreCommandBehavior == "skip" {
					log.Debugf("[%s] StandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				} else if config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
					log.Panicf("[%s] StandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				}
			} else {
				log.Panicf("[%s] receive reply failed. cmd=[%s], error=[%v]", w.stat.Name, e.String(), err)
			}
		}
		if strings.EqualFold(e.CmdName, "select") { // skip select command
			continue
		}
		atomic.AddInt64(&w.stat.UnansweredBytes, -e.SerializedSize)
		atomic.AddInt64(&w.stat.UnansweredEntries, -1)
	}
	w.chWaitWg.Done()
	slog.Debug("receive redis reply end", slog.Int64("count", count))
}

func (w *StandaloneWriter) Status() interface{} {
	return w.stat
}

func (w *StandaloneWriter) StatusString() string {
	return fmt.Sprintf("[%s]: unanswered_entries=%d", w.stat.Name, atomic.LoadInt64(&w.stat.UnansweredEntries))
}

func (w *StandaloneWriter) StatusConsistent() bool {
	return atomic.LoadInt64(&w.stat.UnansweredBytes) == 0 && atomic.LoadInt64(&w.stat.UnansweredEntries) == 0
}
