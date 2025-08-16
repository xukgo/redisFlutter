package redisModels

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ParseMultilineToMap01(t *testing.T) {
	var text = `
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:3
cluster_size:3
cluster_current_epoch:2
cluster_my_epoch:1
cluster_stats_messages_ping_sent:1823152
cluster_stats_messages_pong_sent:1820637
cluster_stats_messages_meet_sent:14
cluster_stats_messages_fail_sent:2
cluster_stats_messages_sent:3643805
cluster_stats_messages_ping_received:1820624
cluster_stats_messages_pong_received:1823081
cluster_stats_messages_meet_received:13
cluster_stats_messages_fail_received:2
cluster_stats_messages_received:3643720
total_cluster_links_buffer_limit_exceeded:0
`

	dict, err := ParseMultilineToMap("tester01", text)
	if err != nil {
		t.Error(err)
		return
	}
	val, find := dict["cluster_state"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "ok")

	val, find = dict["cluster_size"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "3")

	val, find = dict["total_cluster_links_buffer_limit_exceeded"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "0")
}

func Test_ParseMultilineToMap02(t *testing.T) {
	var text = `

# Replication
role:master
connected_slaves:0
master_failover_state:no-failover
master_replid:9191fafa296849ee73e9273959f3b79d65e9dcce
master_replid2:0000000000000000000000000000000000000000
master_repl_offset:0
second_repl_offset:-1
repl_backlog_active:0
repl_backlog_size:1048576
repl_backlog_first_byte_offset:0
repl_backlog_histlen:0

# Errorstats
errorstat_CLUSTERDOWN:count=442
errorstat_ERR:count=29

# Cluster
cluster_enabled:1

# Keyspace
db0:keys=15,expires=0,avg_ttl=0

`

	dict, err := ParseMultilineToMap("tester01", text)
	if err != nil {
		t.Error(err)
		return
	}
	val, find := dict["role"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "master")

	val, find = dict["errorstat_CLUSTERDOWN"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "count=442")

	val, find = dict["errorstat_ERR"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "count=29")

	val, find = dict["cluster_enabled"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "1")

	val, find = dict["db0"]
	if find != true {
		t.FailNow()
	}
	assert.Equal(t, val, "keys=15,expires=0,avg_ttl=0")
}
