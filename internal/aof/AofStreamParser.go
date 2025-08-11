package aof

import (
	"bufio"
	"io"
	"log/slog"
	"redisFlutter/internal/client"
	"redisFlutter/internal/client/proto"
	"redisFlutter/internal/entry"
	"strconv"
	"strings"
)

type AofStreamParser struct {
	protoReader *proto.Reader
	dbId        int
}

func NewAofStreamParser(r io.Reader) *AofStreamParser {
	protoReader := proto.NewReader(bufio.NewReader(r))
	return &AofStreamParser{
		protoReader: protoReader,
		dbId:        0,
	}
}

func (c *AofStreamParser) ParseNext(e *entry.Entry) error {
	iArgv, err := c.protoReader.ReadReply()
	if err != nil {
		slog.Error("AofStreamParser protoReader.ReadReply return error", slog.String("error", err.Error()))
		return err
	}

	argv := client.ArrayString(iArgv, nil)

	// select
	if strings.EqualFold(argv[0], "select") {
		DbId, err := strconv.Atoi(argv[1])
		if err != nil {
			slog.Error("AofStreamParser protoReader.ReadReply select command but argv[1] is invalid numberic string", slog.String("argv[1]", argv[1]), slog.String("error", err.Error()))
			return err
		}
		c.dbId = DbId
	}

	e.Reset()
	e.Argv = argv
	e.DbId = c.dbId
	return nil
}
