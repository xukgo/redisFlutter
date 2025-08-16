package redisModels

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net"
	"strconv"
	"strings"
	"time"
)

// RedisNodeInfo 表示 Redis 节点信息
type RedisNodeInfo struct {
	Address    string `json:"address"`
	Role       string `json:"role"`            // master/slave/sentinel
	ReplID     string `json:"repl_id"`         // 复制ID
	ReplOffset int64  `json:"repl_offset"`     // 复制偏移量
	Slots      string `json:"slots,omitempty"` // 槽位分配 (仅集群)
}

// RedisTopology 表示 Redis 部署拓扑信息
type RedisTopology struct {
	Mode         string          `json:"mode"`                  // single/cluster/sentinel
	MasterName   string          `json:"master_name,omitempty"` // Sentinel 专用
	Nodes        []RedisNodeInfo `json:"nodes"`
	ErrorMessage string          `json:"error,omitempty"`
}

// formatAddress 格式化节点地址，确保IPv6地址正确封装
func formatAddress(host string, port int) string {
	// 确保端口有效
	if port <= 0 || port > 65535 {
		port = 6379
	}

	// 处理IPv6地址
	if strings.Contains(host, ":") && !strings.Contains(host, "[") {
		return net.JoinHostPort(host, strconv.Itoa(port))
	}

	// 默认处理
	return fmt.Sprintf("%s:%d", host, port)
}

// parseNodeAddress 解析节点地址字符串
func parseNodeAddress(addr string) string {
	parts := strings.Split(addr, "@")
	if len(parts) > 0 {
		addr = parts[0]
	}

	// 处理IPv6地址
	if strings.HasPrefix(addr, "[") && strings.Contains(addr, "]:") {
		return addr
	}

	// 处理标准格式
	if strings.Count(addr, ":") > 1 && !strings.HasPrefix(addr, "[") {
		lastColon := strings.LastIndex(addr, ":")
		host := addr[:lastColon]
		port := addr[lastColon+1:]

		// 移除可能存在的协议前缀
		host = strings.TrimPrefix(host, "tcp://")

		// 封装IPv6地址
		if strings.Contains(host, ":") {
			return "[" + host + "]:" + port
		}
		return host + ":" + port
	}

	return addr
}

// DetectRedisTopology 检测 Redis 部署模式并获取所有节点信息
//func DetectRedisTopology(ctx context.Context, addr string) *RedisTopology {
//	// 先尝试作为单实例/集群连接
//	result := detectClusterOrSingle(ctx, addr)
//
//	// 如果失败，尝试作为 Sentinel 检测
//	//if result.ErrorMessage != "" || len(result.Nodes) == 0 {
//	//	if sentinelResult := detectSentinel(ctx, addr); sentinelResult != nil {
//	//		return sentinelResult
//	//	}
//	//}
//
//	return result
//}

// 检测集群或单实例模式
//func detectClusterOrSingle(ctx context.Context, addr string) *RedisTopology {
//	// 创建集群客户端（兼容单实例）
//	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
//		Addrs:       []string{addr},
//		DialTimeout: 3 * time.Second,
//		ReadTimeout: 2 * time.Second,
//		PoolSize:    1,
//	})
//
//	// 确保关闭连接
//	defer clusterClient.Close()
//
//	// 尝试获取集群信息
//	scmd := clusterClient.ClusterInfo(ctx)
//	if scmd.Err() != nil {
//		return nil, scmd.Err()
//	}
//	clusterInfo, err := scmd.Result()
//	if err == nil && len(clusterInfo) > 0 {
//		fmt.Println(clusterInfo)
//		return getClusterTopology(ctx, clusterClient)
//	}
//
//	// 如果不是集群，尝试作为单实例处理
//	return getSingleInstanceTopology(ctx, addr)
//
//}

// 获取集群拓扑信息
func getClusterTopology(ctx context.Context, client *redis.ClusterClient) *RedisTopology {
	topology := &RedisTopology{Mode: "cluster"}

	// 获取集群节点信息
	nodesResult, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		topology.ErrorMessage = fmt.Sprintf("ClusterNodes failed: %v", err)
		return topology
	}
	fmt.Println("result:")
	fmt.Println(nodesResult)

	// 解析节点信息
	lines := strings.Split(nodesResult, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, " ")
		if len(parts) < 8 {
			continue
		}

		nodeAddr := parseNodeAddress(parts[1])
		node := RedisNodeInfo{
			Address: nodeAddr,
			Role:    "slave",
		}

		// 解析节点角色
		if strings.Contains(parts[2], "master") {
			node.Role = "master"
			// 解析槽位分配
			if len(parts) > 8 {
				node.Slots = strings.Join(parts[8:], " ")
			}
		}

		// 解析复制信息
		if len(parts) > 3 {
			fields := strings.Split(parts[3], ",")
			for _, field := range fields {
				if strings.HasPrefix(field, "repl_offset=") {
					fmt.Sscanf(field, "repl_offset=%d", &node.ReplOffset)
				} else if strings.HasPrefix(field, "replid=") {
					node.ReplID = strings.TrimPrefix(field, "replid=")
				}
			}
		}

		topology.Nodes = append(topology.Nodes, node)
	}

	return topology
}

// 获取单实例拓扑信息
func getSingleInstanceTopology(ctx context.Context, addr string) *RedisTopology {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 3 * time.Second,
		PoolSize:    1,
	})
	defer client.Close()

	topology := &RedisTopology{Mode: "single"}

	// 检查是否实际是 Sentinel
	roleResult, err := client.Do(ctx, "ROLE").Result()
	if err == nil {
		if roleList, ok := roleResult.([]interface{}); ok && len(roleList) > 0 {
			if role, ok := roleList[0].(string); ok && role == "sentinel" {
				topology.ErrorMessage = "Node is a Sentinel instance"
				return topology
			}
		}
	}

	// 获取复制信息
	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		topology.ErrorMessage = fmt.Sprintf("INFO replication failed: %v", err)
		return topology
	}
	fmt.Println("replication:")
	fmt.Println(info)

	// 解析复制信息
	node := RedisNodeInfo{Address: addr}
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "role:"):
			node.Role = strings.TrimPrefix(line, "role:")
		case strings.HasPrefix(line, "master_replid:"):
			node.ReplID = strings.TrimPrefix(line, "master_replid:")
		case strings.HasPrefix(line, "master_repl_offset:"):
			fmt.Sscanf(line, "master_repl_offset:%d", &node.ReplOffset)
		}
	}

	topology.Nodes = []RedisNodeInfo{node}
	return topology
}

// 检测 Sentinel 模式
//func detectSentinel(ctx context.Context, addr string) *RedisTopology {
//	sentinelClient := redis.NewSentinelClient(&redis.Options{
//		Addr:        addr,
//		DialTimeout: 5 * time.Second,
//	})
//	defer sentinelClient.Close()
//
//	// 获取 Sentinel 监控的主节点列表
//	masters, err := sentinelClient.Masters(ctx).Result()
//	if err != nil {
//		return &RedisTopology{
//			Mode:         "sentinel",
//			ErrorMessage: fmt.Sprintf("SENTINEL MASTERS failed: %v", err),
//		}
//	}
//
//	if len(masters) == 0 {
//		return &RedisTopology{
//			Mode:         "sentinel",
//			ErrorMessage: "no masters found",
//		}
//	}
//
//	// 获取第一个主节点的名称
//	masterName, ok := masters[0]["name"].(string)
//	if !ok || masterName == "" {
//		return &RedisTopology{
//			Mode:         "sentinel",
//			ErrorMessage: "invalid master name",
//		}
//	}
//
//	// 获取主节点详细信息
//	masterInfo, err := sentinelClient.Master(ctx, masterName).Result()
//	if err != nil {
//		return &RedisTopology{
//			Mode:         "sentinel",
//			MasterName:   masterName,
//			ErrorMessage: fmt.Sprintf("SENTINEL MASTER failed: %v", err),
//		}
//	}
//
//	// 获取该主节点的所有 Sentinel 实例
//	sentinels, err := sentinelClient.Sentinels(ctx, masterName).Result()
//	if err != nil {
//		return &RedisTopology{
//			Mode:         "sentinel",
//			MasterName:   masterName,
//			ErrorMessage: fmt.Sprintf("SENTINEL SENTINELS failed: %v", err),
//		}
//	}
//
//	// 获取从节点信息
//	slaves, err := sentinelClient.Slaves(ctx, masterName).Result()
//	if err != nil {
//		return &RedisTopology{
//			Mode:         "sentinel",
//			MasterName:   masterName,
//			ErrorMessage: fmt.Sprintf("SENTINEL SLAVES failed: %v", err),
//		}
//	}
//
//	// 构建拓扑信息
//	topology := &RedisTopology{
//		Mode:       "sentinel",
//		MasterName: masterName,
//	}
//
//	// 添加主节点
//	if ip, ok := masterInfo["ip"].(string); ok {
//		port, _ := strconv.Atoi(fmt.Sprintf("%v", masterInfo["port"]))
//		nodeAddr := formatAddress(ip, port)
//		topology.Nodes = append(topology.Nodes, RedisNodeInfo{
//			Address: nodeAddr,
//			Role:    "master",
//		})
//	}
//
//	// 添加从节点
//	for _, slave := range slaves {
//		if ip, ok := slave["ip"].(string); ok {
//			port, _ := strconv.Atoi(fmt.Sprintf("%v", slave["port"]))
//			nodeAddr := formatAddress(ip, port)
//			topology.Nodes = append(topology.Nodes, RedisNodeInfo{
//				Address: nodeAddr,
//				Role:    "slave",
//			})
//		}
//	}
//
//	// 添加 Sentinel 节点
//	for _, sentinel := range sentinels {
//		if ip, ok := sentinel["ip"].(string); ok {
//			port, _ := strconv.Atoi(fmt.Sprintf("%v", sentinel["port"]))
//			nodeAddr := formatAddress(ip, port)
//			topology.Nodes = append(topology.Nodes, RedisNodeInfo{
//				Address: nodeAddr,
//				Role:    "sentinel",
//			})
//		}
//	}
//
//	// 添加当前 Sentinel 节点本身
//	topology.Nodes = append(topology.Nodes, RedisNodeInfo{
//		Address: addr,
//		Role:    "sentinel",
//	})
//
//	return topology
//}

//func printRedisTopology(addr string) {
//	ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second*3)
//	defer cancel0()
//
//	fmt.Printf("\n=== Detecting topology for %s ===\n", addr)
//	topology := redisModels.DetectRedisTopology(ctx0, addr)
//
//	fmt.Printf("Mode: %s\n", topology.Mode)
//	if topology.MasterName != "" {
//		fmt.Printf("Master Name: %s\n", topology.MasterName)
//	}
//	if topology.ErrorMessage != "" {
//		fmt.Printf("Error: %s\n", topology.ErrorMessage)
//	}
//
//	fmt.Println("Nodes:")
//	for i, node := range topology.Nodes {
//		fmt.Printf("  %d. Address: %-25s Role: %-10s", i+1, node.Address, node.Role)
//		if node.ReplID != "" {
//			fmt.Printf(" ReplID: %s", node.ReplID)
//		}
//		if node.ReplOffset > 0 {
//			fmt.Printf(" Offset: %d", node.ReplOffset)
//		}
//		if node.Slots != "" {
//			fmt.Printf(" Slots: %s", node.Slots)
//		}
//		fmt.Println()
//	}
//
//}
