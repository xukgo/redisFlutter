package entry

import (
	"bytes"
	"strings"

	"redisFlutter/internal/client/proto"
	"redisFlutter/internal/commands"
	"redisFlutter/internal/log"
)

type Entry struct {
	DbId int      // required
	Argv []string // required

	CmdName    string
	Group      string
	Keys       []string
	KeyIndexes []int
	Slots      []int

	// for stat
	SerializedSize int64
}

func (e *Entry) Reset() {
	e.DbId = 0
	e.CmdName = ""
	e.Group = ""

	e.Argv = e.Argv[:0]
	e.Keys = e.Keys[:0]
	e.KeyIndexes = e.KeyIndexes[:0]
	e.Slots = e.Slots[:0]
	e.SerializedSize = 0
}

func NewEntry() *Entry {
	e := new(Entry)
	e.Argv = make([]string, 0, 4)
	e.Keys = make([]string, 0, 4)
	e.KeyIndexes = make([]int, 0, 4)
	e.Slots = make([]int, 0, 4)
	return e
}

func (e *Entry) Clone() *Entry {
	m := new(Entry)
	m.DbId = e.DbId
	m.CmdName = e.CmdName
	m.Group = e.Group
	m.SerializedSize = e.SerializedSize

	m.Argv = make([]string, 0, len(e.Argv))
	m.Argv = append(m.Argv, e.Argv...)
	m.Keys = make([]string, 0, len(e.Keys))
	m.Keys = append(m.Keys, e.Keys...)
	m.KeyIndexes = make([]int, 0, len(e.KeyIndexes))
	m.KeyIndexes = append(m.KeyIndexes, e.KeyIndexes...)
	m.Slots = make([]int, 0, len(e.Slots))
	m.Slots = append(m.Slots, e.Slots...)
	return m
}
func (e *Entry) String() string {
	str := strings.Join(e.Argv, " ")
	if len(str) > 100 {
		str = str[:100] + "..."
	}
	return str
}

func (e *Entry) Serialize() []byte {
	buf := new(bytes.Buffer)
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(e.Argv))

	for inx, item := range e.Argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.Panicf(err.Error())
	}
	e.SerializedSize = int64(buf.Len())
	return buf.Bytes()
}

func (e *Entry) Parse() {
	e.CmdName, e.Group, e.Keys, e.KeyIndexes = commands.CalcKeys(e.Argv)
	e.Slots = commands.CalcSlots(e.Keys)
}
