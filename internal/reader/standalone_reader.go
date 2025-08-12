package reader

import (
	"bytes"
	"context"
	"github.com/dustin/go-humanize"
	"io"
	"os"
	"path/filepath"
	"redisFlutter/internal/client"
	"redisFlutter/internal/config"
	"redisFlutter/internal/log"
	"redisFlutter/internal/utils"
	rotate "redisFlutter/internal/utils/file_rotate"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type StandaloneReader struct {
	opts *SyncReaderOptions

	ctx    context.Context
	client *client.Redis
	DbId   int
	stat   syncStandaloneReaderStat

	isDiskless bool
	//writeCache   *IntervalMaxSizeCache
	//aofStorage   io.Writer
	//aofSaveIndex uint64
}

func NewStandaloneReader(ctx context.Context, opts *SyncReaderOptions) (*StandaloneReader, error) {
	var err error
	c := new(StandaloneReader)
	c.opts = opts
	//c.aofStorage = aofStorage
	//c.aofSaveIndex = 0
	//c.writeCache = NewIntervalMaxSizeCache(time.Millisecond*500, 16*1024)
	c.client, err = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)
	if err != nil {
		return nil, err
	}

	c.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	c.stat.Address = opts.Address
	c.stat.Status = kHandShake

	saveDirPath, _ := filepath.Abs(opts.DataDirPath)
	c.stat.Dir = saveDirPath //filepath.Join(saveDirPath, c.stat.Name)
	utils.CreateEmptyDir(c.stat.Dir)

	return c, nil
}

func (r *StandaloneReader) supportPSync() bool {
	reply := r.client.DoWithStringReply("info", "server")
	for _, line := range strings.Split(reply, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			version := strings.Split(line, ":")[1]
			parts := strings.Split(version, ".")
			if len(parts) > 2 {
				v1, _ := strconv.Atoi(parts[0])
				v2, _ := strconv.Atoi(parts[1])
				if v1*1000+v2 < 2008 {
					return false
				}
			}

		}
	}

	return true
}

func (r *StandaloneReader) StartRead(ctx context.Context) {
	if r.supportPSync() { // Redis version >= 2.8
		r.StartReadWithPSync(ctx)
	} else { // Redis version < 2.8
		r.StartReadWithSync(ctx)
	}
}

// StartReadWithPSync is used in Redis version >= 2.8
func (r *StandaloneReader) StartReadWithPSync(ctx context.Context) {
	r.ctx = ctx
	go func() {
		r.sendReplconfListenPort()
		r.sendPSync()
		r.receiveRDB()
		//rdbFilePath := r.receiveRDB()
		//startOffset := r.stat.AofReceivedOffset
		go r.sendReplconfAck() // start sent replconf ack
		go r.receiveAOF()
	}()

}

// StartReadWithSync is only used in Redis version < 2.8
func (r *StandaloneReader) StartReadWithSync(ctx context.Context) {
	r.ctx = ctx
	go func() {
		r.sendSync()
		r.receiveRDB()
		//rdbFilePath := r.receiveRDB()
		//startOffset := r.stat.AofReceivedOffset
		go r.receiveAOF()
	}()
}

func (r *StandaloneReader) sendReplconfListenPort() {
	// use status_port as redis-shake port
	argv := []interface{}{"replconf", "listening-port", strconv.Itoa(config.Opt.Advanced.StatusPort)}
	r.client.Send(argv...)
	_, err := r.client.Receive()
	if err != nil {
		log.Warnf("[%s] send replconf command to redis server failed. error=[%v]", r.stat.Name, err)
	}
}

// When BGSAVE is triggered by the source Redis itself, synchronization is blocked, so need to check it
func (r *StandaloneReader) checkBgsaveInProgress() {
	for {
		select {
		case <-r.ctx.Done():
			runtime.Goexit() // stop goroutine
		default:
			argv := []interface{}{"INFO", "persistence"}
			r.client.Send(argv...)
			receiveString := r.client.ReceiveString()
			if strings.Contains(receiveString, "rdb_bgsave_in_progress:1") || strings.Contains(receiveString, "aof_rewrite_in_progress:1") {
				log.Warnf("[%s] source db is doing bgsave, waiting for a while.", r.stat.Name)
			} else {
				log.Infof("[%s] source db is not doing bgsave! continue.", r.stat.Name)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (r *StandaloneReader) sendPSync() {
	if r.opts.TryDiskless {
		argv := []interface{}{"REPLCONF", "CAPA", "EOF"}
		reply := r.client.DoWithStringReply(argv...)
		if reply != "OK" {
			log.Warnf("[%s] send replconf capa eof to redis server failed. reply=[%v]", r.stat.Name, reply)
		} else {
			r.isDiskless = true
		}
	}
	r.checkBgsaveInProgress()
	// send PSync
	argv := []interface{}{"PSYNC", "?", "-1"}
	if config.Opt.Advanced.AwsPSync != "" {
		argv = []interface{}{config.Opt.Advanced.GetPSyncCommand(r.stat.Address), "?", "-1"}
	}
	r.client.Send(argv...)

	// format: \n\n\n+<reply>\r\n
	for {
		select {
		case <-r.ctx.Done():
			//close(r.ch)
			runtime.Goexit() // stop goroutine
		default:
		}
		peakByte, err := r.client.Peek()
		if err != nil {
			log.Panicf(err.Error())
		}
		if peakByte != '\n' {
			break
		}
		_, err = r.client.ReadByte()
		if err != nil {
			log.Panicf("[%s] pop byte failed. error=[%v]", r.stat.Name, err)
		}
	}
	reply := r.client.ReceiveString()
	masterOffset, err := strconv.Atoi(strings.Split(reply, " ")[2])
	if err != nil {
		log.Panicf(err.Error())
	}
	r.stat.AofReceivedOffset = int64(masterOffset)
}

func (r *StandaloneReader) sendSync() {
	if r.opts.TryDiskless {
		argv := []interface{}{"REPLCONF", "CAPA", "EOF"}
		reply := r.client.DoWithStringReply(argv...)
		if reply != "OK" {
			log.Warnf("[%s] send replconf capa eof to redis server failed. reply=[%v]", r.stat.Name, reply)
		}
	}
	r.checkBgsaveInProgress()
	// send SYNC
	argv := []interface{}{"SYNC"}
	if config.Opt.Advanced.AwsPSync != "" {
		argv = []interface{}{config.Opt.Advanced.GetPSyncCommand(r.stat.Address), "?", "-1"}
	}
	r.client.Send(argv...)

	// format: \n\n\n+<reply>\r\n
	for {
		select {
		case <-r.ctx.Done():
			//close(r.ch)
			runtime.Goexit() // stop goroutine
		default:
		}
		peekByte, err := r.client.Peek()
		if err != nil {
			log.Panicf(err.Error())
		}
		if peekByte != '\n' {
			break
		}
		_, err = r.client.ReadByte()
		if err != nil {
			log.Panicf("[%s] pop byte failed. error=[%v]", r.stat.Name, err)
		}
	}
}

func (r *StandaloneReader) receiveRDB() string {
	log.Debugf("[%s] source db is doing bgsave.", r.stat.Name)
	r.stat.Status = kWaitBgsave
	timeStart := time.Now()
	// format: \n\n\n$<length>\r\n<rdb>
	// if source support repl-diskless-sync: \n\n\n$EOF:<40 characters EOF marker>\r\nstream data<EOF marker>
	for {
		b, err := r.client.ReadByte()
		if err != nil {
			log.Panicf(err.Error())
		}
		if b == '\n' { // heartbeat
			continue
		}
		if b != '$' {
			log.Panicf("[%s] invalid rdb format. b=[%s]", r.stat.Name, string(b))
		}
		break
	}
	log.Debugf("[%s] source db bgsave finished. timeUsed=[%d]ms", r.stat.Name, time.Since(timeStart).Milliseconds())
	marker, err := r.client.ReadString('\n')
	if err != nil {
		log.Panicf(err.Error())
	}
	marker = strings.TrimSpace(marker)

	// create rdb file
	rdbFilePath := filepath.Join(r.stat.Dir, "dump.rdb")
	if err != nil {
		log.Panicf(err.Error())
	}
	timeStart = time.Now()
	log.Debugf("[%s] start receiving RDB. path=[%s]", r.stat.Name, rdbFilePath)
	rdbFileHandle, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		log.Panicf(err.Error())
	}

	// receive rdb
	r.stat.Status = kReceiveRdb
	if strings.HasPrefix(marker, "EOF") {
		log.Infof("[%s] source db supoort diskless sync capability.", r.stat.Name)
		r.receiveRDBWithDiskless(marker, rdbFileHandle)
	} else {
		r.receiveRDBWithoutDiskless(marker, rdbFileHandle)
	}
	err = rdbFileHandle.Close()
	if err != nil {
		log.Panicf(err.Error())
	}
	log.Debugf("[%s] save RDB finished. timeUsed=[%.2f]s", r.stat.Name, time.Since(timeStart).Seconds())
	return rdbFilePath
}

func (r *StandaloneReader) receiveRDBWithDiskless(marker string, wt io.Writer) {
	const bufSize int64 = 32 * 1024 * 1024 // 32MB
	buf := make([]byte, bufSize)

	marker = strings.Split(marker, ":")[1]
	if len(marker) != RDB_EOF_MARKER_LEN {
		log.Panicf("[%s] invalid len of EOF marker. value=[%s]", r.stat.Name, marker)
	}
	log.Infof("meet EOF begin marker: %s", marker)
	bMarker := []byte(marker)
	var lastBytes []byte
	for {
		copy(buf, lastBytes) // copy previous tail bytes to head of buf

		nread, err := r.client.Read(buf[len(lastBytes):])
		if err != nil {
			log.Panicf(err.Error())
		}

		bufLen := len(lastBytes) + nread
		nwrite := 0
		if bufLen >= RDB_EOF_MARKER_LEN && bytes.Equal(buf[bufLen-RDB_EOF_MARKER_LEN:bufLen], bMarker) {
			log.Infof("meet EOF end marker.")
			// Write all buf without EOF marker and break
			if nwrite, err = wt.Write(buf[:bufLen-RDB_EOF_MARKER_LEN]); err != nil {
				log.Panicf(err.Error())
			}
			break
		}

		if bufLen >= RDB_EOF_MARKER_LEN {
			// left RDB_EOF_MARKER_LEN bytes to next round
			if nwrite, err = wt.Write(buf[:bufLen-RDB_EOF_MARKER_LEN]); err != nil {
				log.Panicf(err.Error())
			}
			lastBytes = buf[bufLen-RDB_EOF_MARKER_LEN : bufLen] // save last RDB_EOF_MARKER_LEN bytes into lastBytes for next round
		} else {
			// save all bytes into lastBytes for next round if less than RDB_EOF_MARKER_LEN
			lastBytes = buf[:bufLen]
		}

		r.stat.RdbFileSizeBytes += uint64(nwrite)
		r.stat.RdbReceivedBytes += uint64(nwrite)
	}
}

func (r *StandaloneReader) receiveRDBWithoutDiskless(marker string, wt io.Writer) {
	length, err := strconv.ParseInt(marker, 10, 64)
	if err != nil {
		log.Panicf(err.Error())
	}
	log.Debugf("[%s] rdb file size: [%v]", r.stat.Name, humanize.IBytes(uint64(length)))
	r.stat.RdbFileSizeBytes = uint64(length)

	remainder := length
	const bufSize int64 = 32 * 1024 * 1024 // 32MB
	buf := make([]byte, bufSize)
	for remainder != 0 {
		readOnce := bufSize
		if remainder < readOnce {
			readOnce = remainder
		}
		n, err := r.client.Read(buf[:readOnce])
		if err != nil {
			log.Panicf(err.Error())
		}
		remainder -= int64(n)
		_, err = wt.Write(buf[:n])
		if err != nil {
			log.Panicf(err.Error())
		}

		r.stat.RdbReceivedBytes += uint64(n)
	}
}

func (r *StandaloneReader) receiveAOF() {
	log.Debugf("[%s] start receiving aof data, and save to file", r.stat.Name)
	aofWriter := rotate.NewAOFWriter(r.stat.Name, r.stat.Dir, r.stat.AofReceivedOffset)
	defer aofWriter.Close()

	//once := new(sync.Once)
	buf := make([]byte, 16*1024) // 16KB is enough for writing file
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			n, err := r.client.Read(buf)
			if err != nil {
				log.Panicf(err.Error())
			}
			r.stat.AofReceivedBytes += uint64(n)
			//log.Debugf("[%s] receiving aof data len = %d", r.stat.Name, n)
			aofWriter.Write(buf[:n])
			r.stat.AofReceivedOffset += int64(n)
		}
	}
}

// sendReplconfAck sends replconf ack to master to maintain heartbeat between redis-shake and source redis.
func (r *StandaloneReader) sendReplconfAck() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.stat.AofReceivedOffset != 0 {
				r.client.Send("replconf", "ack", strconv.FormatInt(r.stat.AofReceivedOffset, 10))
			}
		}
	}
}

//func (r *StandaloneReader) nextKey() []byte {
//	key := make([]byte, 8)
//	binary.BigEndian.PutUint64(key, r.aofSaveIndex)
//	r.aofSaveIndex++
//	return key
//}
