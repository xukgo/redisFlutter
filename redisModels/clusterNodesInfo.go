package redisModels

import (
	"bufio"
	"github.com/samber/lo"
	"io"
	"log/slog"
	"math"
	"strings"
)

type ClusterNodesInfo struct {
	Nodes []ClusterNodeInfo
}

func NewClusterNodesInfoFromLines(text string) (*ClusterNodesInfo, error) {
	list := make([]ClusterNodeInfo, 0, 18)
	reader := bufio.NewReader(strings.NewReader(text))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("Parse ClusterNodesInfo read line error", slog.String("error", err.Error()), slog.String("text", text))
			return nil, err
		}

		// 去除换行符
		line = strings.Trim(line, "\n")
		line = strings.Trim(line, "\r")
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		nodeInfo, err := NewClusterNodeInfoFromLine(line)
		if err != nil {
			return nil, err
		}
		list = append(list, *nodeInfo)
	}

	sinfo := &ClusterNodesInfo{Nodes: list}
	return sinfo, nil
}

func (c *ClusterNodesInfo) CheckAllMasterReady() bool {
	var notReadyCount = 0
	lo.ForEach(c.Nodes, func(item ClusterNodeInfo, index int) {
		if item.CheckRoleMaster() && !item.CheckStateReady() {
			notReadyCount++
		}
	})
	return notReadyCount == 0
}

func (c *ClusterNodesInfo) GetMasterCount() int {
	var count = 0
	lo.ForEach(c.Nodes, func(item ClusterNodeInfo, index int) {
		if item.CheckRoleMaster() {
			count++
		}
	})
	return count
}
func (c *ClusterNodesInfo) GetClusterSize() int {
	arr := make([]int, 0, 9)
	for _, info := range c.Nodes {
		sub := info.SlotRange.End - info.SlotRange.Start
		if sub > 0 {
			arr = append(arr, sub)
		}
	}
	if len(arr) == 0 {
		return -1
	}
	sum := lo.Sum(arr)
	var average = float64(sum) / float64(len(arr))
	size := int(math.Round(16384.0 / average))
	return size
}
