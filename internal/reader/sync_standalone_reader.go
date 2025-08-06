package reader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"redisFlutter/internal/client/proto"

	"redisFlutter/internal/client"
	"redisFlutter/internal/config"
	"redisFlutter/internal/entry"
	"redisFlutter/internal/log"
	"redisFlutter/internal/rdb"
	"redisFlutter/internal/utils"
	rotate "redisFlutter/internal/utils/file_rotate"

	"github.com/dustin/go-humanize"
)

type SyncReaderOptions struct {
	Cluster       bool                   `mapstructure:"cluster" default:"false"`
	Address       string                 `mapstructure:"address" default:""`
	Username      string                 `mapstructure:"username" default:""`
	Password      string                 `mapstructure:"password" default:""`
	Tls           bool                   `mapstructure:"tls" default:"false"`
	TlsConfig     client.TlsConfig       `mapstructure:"tls_config" default:"{}"`
	SyncRdb       bool                   `mapstructure:"sync_rdb" default:"true"`
	SyncAof       bool                   `mapstructure:"sync_aof" default:"true"`
	PreferReplica bool                   `mapstructure:"prefer_replica" default:"false"`
	TryDiskless   bool                   `mapstructure:"try_diskless" default:"false"`
	Sentinel      client.SentinelOptions `mapstructure:"sentinel"`
}

const RDB_EOF_MARKER_LEN = 40

type State string

const (
	kHandShake  State = "hand shaking"
	kWaitBgsave State = "waiting bgsave"
	kReceiveRdb State = "receiving rdb"
	kSyncRdb    State = "syncing rdb"
	kSyncAof    State = "syncing aof"
)

type syncStandaloneReaderStat struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Dir     string `json:"dir"`

	// status
	Status State `json:"status"`

	// rdb info
	RdbFileSizeBytes uint64 `json:"rdb_file_size_bytes"` // bytes of the rdb file
	RdbFileSizeHuman string `json:"rdb_file_size_human"`
	RdbReceivedBytes uint64 `json:"rdb_received_bytes"` // bytes of RDB received from master
	RdbReceivedHuman string `json:"rdb_received_human"`
	RdbSentBytes     uint64 `json:"rdb_sent_bytes"` // bytes of RDB sent to chan
	RdbSentHuman     string `json:"rdb_sent_human"`

	// aof info
	AofReceivedOffset int64  `json:"aof_received_offset"` // offset of AOF received from master
	AofSentOffset     int64  `json:"aof_sent_offset"`     // offset of AOF sent to chan
	AofReceivedBytes  uint64 `json:"aof_received_bytes"`  // bytes of AOF received from master
	AofReceivedHuman  string `json:"aof_received_human"`
}

func (s syncStandaloneReaderStat) MarshalJSON() ([]byte, error) {
	if s.RdbFileSizeBytes != 0 {
		s.RdbFileSizeHuman = humanize.IBytes(s.RdbFileSizeBytes)
	}
	if s.RdbReceivedBytes != 0 {
		s.RdbReceivedHuman = humanize.IBytes(s.RdbReceivedBytes)
	}
	if s.RdbSentBytes != 0 {
		s.RdbSentHuman = humanize.IBytes(s.RdbSentBytes)
	}
	if s.AofReceivedBytes != 0 {
		s.AofReceivedHuman = humanize.IBytes(s.AofReceivedBytes)
	}

	type aliasStat syncStandaloneReaderStat // alias to avoid infinite recursion
	return json.Marshal(aliasStat(s))
}

type syncStandaloneReader struct {
	ctx    context.Context
	opts   *SyncReaderOptions
	client *client.Redis

	ch   chan *entry.Entry
	DbId int

	stat syncStandaloneReaderStat

	// version info
	isDiskless bool
}

func NewSyncStandaloneReader(ctx context.Context, opts *SyncReaderOptions) Reader {
	r := new(syncStandaloneReader)
	r.opts = opts
	r.client = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	r.stat.Address = opts.Address
	r.stat.Status = kHandShake
	r.stat.Dir = utils.GetAbsPath(r.stat.Name)
	utils.CreateEmptyDir(r.stat.Dir)

	return r
}

func (r *syncStandaloneReader) supportPSync() bool {
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

func (r *syncStandaloneReader) StartRead(ctx context.Context) []chan *entry.Entry {
	if r.supportPSync() { // Redis version >= 2.8
		return r.StartReadWithPSync(ctx)
	} else { // Redis version < 2.8
		return r.StartReadWithSync(ctx)
	}
}

// StartReadWithPSync is used in Redis version >= 2.8
func (r *syncStandaloneReader) StartReadWithPSync(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	r.ch = make(chan *entry.Entry, 1024)
	go func() {
		r.sendReplconfListenPort()
		r.sendPSync()
		rdbFilePath := r.receiveRDB()
		startOffset := r.stat.AofReceivedOffset
		go r.sendReplconfAck() // start sent replconf ack
		go r.receiveAOF()
		if r.opts.SyncRdb {
			r.sendRDB(rdbFilePath)
		}
		if r.opts.SyncAof {
			r.stat.Status = kSyncAof
			r.sendAOF(startOffset)
		}
		close(r.ch)
	}()

	return []chan *entry.Entry{r.ch}
}

// StartReadWithSync is only used in Redis version < 2.8
func (r *syncStandaloneReader) StartReadWithSync(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	r.ch = make(chan *entry.Entry, 1024)
	go func() {
		r.sendSync()
		rdbFilePath := r.receiveRDB()
		startOffset := r.stat.AofReceivedOffset
		go r.receiveAOF()
		if r.opts.SyncRdb {
			r.sendRDB(rdbFilePath)
		}
		if r.opts.SyncAof {
			r.stat.Status = kSyncAof
			r.sendAOF(startOffset)
		}
		close(r.ch)
	}()

	return []chan *entry.Entry{r.ch}
}

func (r *syncStandaloneReader) sendReplconfListenPort() {
	// use status_port as redis-shake port
	argv := []interface{}{"replconf", "listening-port", strconv.Itoa(config.Opt.Advanced.StatusPort)}
	r.client.Send(argv...)
	_, err := r.client.Receive()
	if err != nil {
		log.Warnf("[%s] send replconf command to redis server failed. error=[%v]", r.stat.Name, err)
	}
}

// When BGSAVE is triggered by the source Redis itself, synchronization is blocked, so need to check it
func (r *syncStandaloneReader) checkBgsaveInProgress() {
	for {
		select {
		case <-r.ctx.Done():
			close(r.ch)
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
			time.Sleep(1 * time.Second)
		}
	}
}

func (r *syncStandaloneReader) sendPSync() {
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
			close(r.ch)
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

func (r *syncStandaloneReader) sendSync() {
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
			close(r.ch)
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

func (r *syncStandaloneReader) receiveRDB() string {
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
	rdbFilePath, err := filepath.Abs(r.stat.Name + "/dump.rdb")
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

func (r *syncStandaloneReader) receiveRDBWithDiskless(marker string, wt io.Writer) {
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

func (r *syncStandaloneReader) receiveRDBWithoutDiskless(marker string, wt io.Writer) {
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

func (r *syncStandaloneReader) receiveAOF() {
	log.Debugf("[%s] start receiving aof data, and save to file", r.stat.Name)
	aofWriter := rotate.NewAOFWriter(r.stat.Name, r.stat.Dir, r.stat.AofReceivedOffset)
	defer aofWriter.Close()
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
			aofWriter.Write(buf[:n])
			r.stat.AofReceivedOffset += int64(n)
		}
	}
}

func (r *syncStandaloneReader) sendRDB(rdbFilePath string) {
	// start parse rdb
	log.Debugf("[%s] start sending RDB to target", r.stat.Name)
	r.stat.Status = kSyncRdb
	updateFunc := func(offset int64) {
		r.stat.RdbSentBytes = uint64(offset)
	}
	rdbLoader := rdb.NewLoader(r.stat.Name, updateFunc, rdbFilePath, r.ch)
	r.DbId = rdbLoader.ParseRDB(r.ctx)
	log.Debugf("[%s] send RDB finished", r.stat.Name)
	// delete file
	_ = os.Remove(rdbFilePath)
	log.Debugf("[%s] delete RDB file", r.stat.Name)
}

func (r *syncStandaloneReader) sendAOF(offset int64) {
	aofReader := rotate.NewAOFReader(r.ctx, r.stat.Name, r.stat.Dir, offset)
	defer aofReader.Close()
	protoReader := proto.NewReader(bufio.NewReader(aofReader))
	for {
		if err := r.ctx.Err(); err != nil {
			log.Infof("[%s] sendAOF exit", r.stat.Name)
			return
		}

		iArgv, err := protoReader.ReadReply()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Panicf("[%s] read aof file failed. error=[%v]", r.stat.Name, err)
		}

		argv := client.ArrayString(iArgv, nil)
		r.stat.AofSentOffset = aofReader.Offset()
		// select
		if strings.EqualFold(argv[0], "select") {
			DbId, err := strconv.Atoi(argv[1])
			if err != nil {
				log.Panicf(err.Error())
			}
			r.DbId = DbId
			continue
		}
		// ping
		if strings.EqualFold(argv[0], "ping") {
			continue
		}
		// replconf @AWS
		if strings.EqualFold(argv[0], "replconf") {
			continue
		}
		// opinfo @Aliyun
		if strings.EqualFold(argv[0], "opinfo") {
			continue
		}
		// txn
		if strings.EqualFold(argv[0], "multi") || strings.EqualFold(argv[0], "exec") {
			continue
		}
		// sentinel
		if strings.EqualFold(argv[0], "publish") && strings.EqualFold(argv[1], "__sentinel__:hello") {
			continue
		}

		e := entry.NewEntry()
		e.Argv = argv
		e.DbId = r.DbId
		r.ch <- e
	}
}

// sendReplconfAck sends replconf ack to master to maintain heartbeat between redis-shake and source redis.
func (r *syncStandaloneReader) sendReplconfAck() {
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

func (r *syncStandaloneReader) Status() interface{} {
	return r.stat
}

func (r *syncStandaloneReader) StatusString() string {
	if r.stat.Status == kSyncRdb {
		return fmt.Sprintf("%s, size=[%s/%s]", r.stat.Status, humanize.IBytes(r.stat.RdbSentBytes), humanize.IBytes(r.stat.RdbFileSizeBytes))
	}
	if r.stat.Status == kSyncAof {
		return fmt.Sprintf("%s, diff=[%v]", r.stat.Status, -r.stat.AofSentOffset+r.stat.AofReceivedOffset)
	}
	if r.stat.Status == kReceiveRdb {
		if r.isDiskless {
			return fmt.Sprintf("%s diskless, size=[%s]", r.stat.Status, humanize.IBytes(r.stat.RdbReceivedBytes))
		}
		return fmt.Sprintf("%s, size=[%s/%s]", r.stat.Status, humanize.IBytes(r.stat.RdbReceivedBytes), humanize.IBytes(r.stat.RdbFileSizeBytes))
	}
	return string(r.stat.Status)
}

func (r *syncStandaloneReader) StatusConsistent() bool {
	return r.stat.AofReceivedOffset != 0 &&
		r.stat.AofReceivedOffset == r.stat.AofSentOffset &&
		len(r.ch) == 0
}
