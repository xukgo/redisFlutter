package redisModels

import (
	"bufio"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

type SlotRangeInfo struct {
	Start int
	End   int
}

func NewSlotRangeInfoFromLine(text string) (*SlotRangeInfo, error) {
	model := new(SlotRangeInfo)
	text = strings.TrimSpace(text)
	if text == "" {
		return model, nil
	}
	sarr := strings.Split(text, "-")
	if len(sarr) != 2 {
		slog.Error("parse slot range info fail", slog.String("text", text))
		return model, fmt.Errorf("invalid line")
	}
	start, err := strconv.Atoi(sarr[0])
	if err != nil {
		slog.Error("parse slot range start fail", slog.String("text", text))
		return model, err
	}
	end, err := strconv.Atoi(sarr[1])
	if err != nil {
		slog.Error("parse slot range end fail", slog.String("text", text))
		return model, err
	}
	model.Start = start
	model.End = end
	return model, nil
}

type ClusterNodeInfo struct {
	NodeID         string
	Addr           string
	CPort          string
	HostName       string
	Flags          []string //myself, master, slave, fail?, fail, handshake, noaddr, nofailover, noflags
	PrimaryNodeID  string
	PingSentUnixMs string
	PongRecvUnixMs string
	ConfigEpoch    string
	LinkState      string
	SlotRange      SlotRangeInfo
}

func NewClusterNodeInfoFromLine(text string) (*ClusterNodeInfo, error) {
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Split(bufio.ScanWords) // 按单词扫描

	count := 0

	info := new(ClusterNodeInfo)
	for scanner.Scan() {
		switch count {
		case 0:
			info.NodeID = scanner.Text()
		case 1:
			str := scanner.Text()
			sarr := strings.Split(str, ",")
			if len(sarr) > 1 {
				info.HostName = sarr[1]
			}
			sarr = strings.Split(sarr[0], "@")
			if len(sarr) > 1 {
				info.CPort = sarr[1]
			}
			info.Addr = sarr[0]
		case 2:
			str := scanner.Text()
			info.Flags = strings.Split(str, ",")
		case 3:
			info.PrimaryNodeID = scanner.Text()
			if len(info.PrimaryNodeID) < 3 {
				info.PrimaryNodeID = ""
			}
		case 4:
			info.PingSentUnixMs = scanner.Text()
		case 5:
			info.PongRecvUnixMs = scanner.Text()
		case 6:
			info.ConfigEpoch = scanner.Text()
		case 7:
			info.LinkState = scanner.Text()
		case 8:
			rinfo, err := NewSlotRangeInfoFromLine(scanner.Text())
			if err != nil {
				return nil, err
			}
			info.SlotRange = *rinfo
		}
		count++
	}
	return info, nil
}

func (c *ClusterNodeInfo) CheckRoleMaster() bool {
	for idx := range c.Flags {
		if strings.EqualFold(c.Flags[idx], "master") {
			return true
		}
	}
	return false
}

func (c *ClusterNodeInfo) CheckStateReady() bool {
	roleExist := false
	for idx := range c.Flags {
		str := c.Flags[idx]
		if strings.EqualFold(str, "master") {
			roleExist = true
		}
		if strings.EqualFold(str, "slave") {
			roleExist = true
		}
		if strings.Contains(str, "fail") {
			return false
		}
		if strings.Contains(str, "handshake") {
			return false
		}
		if strings.Contains(str, "noaddr") {
			return false
		}
		if strings.Contains(str, "noflags") {
			return false
		}
	}
	return roleExist
}
func (c *ClusterNodeInfo) CheckConnected() bool {
	return strings.EqualFold(c.LinkState, "connected")
}
