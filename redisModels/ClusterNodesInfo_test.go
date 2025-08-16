package redisModels

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ClusterNodesInfoParse02(t *testing.T) {
	var text = `
409edf623943d637f87733ed968068a4b4197566 10.42.132.26:6379@16379,primary-1-1.primary.pqy.svc.cluster.local master,fail? - 0 1755338119109 19 connected 5462-10922
dae532cf05c71ece059267cebd9e200c92a96c02 10.42.52.20:6379@16379,primary-2-1.primary.pqy.svc.cluster.local slave ae97d4725716ce86fdcc235826102cefb5616c9e 0 1755338119109 18 connected
27e1fbec4210960b6d7ffcb627e85b89c92ffbc3 10.42.52.27:6379@16379,primary-0-2.primary.pqy.svc.cluster.local myself,master - 0 1755338119000 7 connected 0-5461
1b30e63cc3c55da0f6d8b2bd0b46e20a622af0a3 10.42.151.24:6379@16379,primary-0-1.primary.pqy.svc.cluster.local slave 27e1fbec4210960b6d7ffcb627e85b89c92ffbc3 0 1755338119109 7 connected
88b051300a574b5635b33eae9db3b0fbd37a3635 10.42.132.6:6379@16379,primary-0-0.primary.pqy.svc.cluster.local slave 27e1fbec4210960b6d7ffcb627e85b89c92ffbc3 0 1755338119109 7 connected
e96aa0dfed8cdd74d98df24fd22043a246c62963 10.42.132.5:6379@16379,primary-2-2.primary.pqy.svc.cluster.local slave ae97d4725716ce86fdcc235826102cefb5616c9e 0 1755338118697 18 connected
18b8a9e915b5339433171c870d58f78745926f86 10.42.51.246:6379@16379,primary-1-0.primary.pqy.svc.cluster.local slave 409edf623943d637f87733ed968068a4b4197566 0 1755338119109 19 connected
bfa4759c572a688efe9e015f38da20d3a2008709 10.42.151.110:6379@16379,primary-1-2.primary.pqy.svc.cluster.local slave 409edf623943d637f87733ed968068a4b4197566 0 1755338119109 19 connected
ae97d4725716ce86fdcc235826102cefb5616c9e 10.42.151.4:6379@16379,primary-2-0.primary.pqy.svc.cluster.local master - 0 1755338119000 18 connected 10923-16383
`
	sinfo, err := NewClusterNodesInfoFromLines(text)
	if err != nil {
		t.Error(err)
		return
	}
	list := sinfo.Nodes
	assert.True(t, sinfo != nil)
	assert.True(t, len(list) == 9)
	assert.True(t, sinfo.GetClusterSize() == 3)
	assert.True(t, sinfo.GetMasterCount() == 3)
	assert.True(t, sinfo.CheckAllMasterReady() == false)
}
func Test_ClusterNodesInfoParse01(t *testing.T) {
	var text = `
409edf623943d637f87733ed968068a4b4197566 10.42.132.26:6379@16379,primary-1-1.primary.pqy.svc.cluster.local master - 0 1755338119109 19 connected 5462-10922
dae532cf05c71ece059267cebd9e200c92a96c02 10.42.52.20:6379@16379,primary-2-1.primary.pqy.svc.cluster.local slave ae97d4725716ce86fdcc235826102cefb5616c9e 0 1755338119109 18 connected
27e1fbec4210960b6d7ffcb627e85b89c92ffbc3 10.42.52.27:6379@16379,primary-0-2.primary.pqy.svc.cluster.local myself,master - 0 1755338119000 7 connected 0-5461
1b30e63cc3c55da0f6d8b2bd0b46e20a622af0a3 10.42.151.24:6379@16379,primary-0-1.primary.pqy.svc.cluster.local slave 27e1fbec4210960b6d7ffcb627e85b89c92ffbc3 0 1755338119109 7 connected
88b051300a574b5635b33eae9db3b0fbd37a3635 10.42.132.6:6379@16379,primary-0-0.primary.pqy.svc.cluster.local slave 27e1fbec4210960b6d7ffcb627e85b89c92ffbc3 0 1755338119109 7 connected
e96aa0dfed8cdd74d98df24fd22043a246c62963 10.42.132.5:6379@16379,primary-2-2.primary.pqy.svc.cluster.local slave ae97d4725716ce86fdcc235826102cefb5616c9e 0 1755338118697 18 connected
18b8a9e915b5339433171c870d58f78745926f86 10.42.51.246:6379@16379,primary-1-0.primary.pqy.svc.cluster.local slave 409edf623943d637f87733ed968068a4b4197566 0 1755338119109 19 connected
bfa4759c572a688efe9e015f38da20d3a2008709 10.42.151.110:6379@16379,primary-1-2.primary.pqy.svc.cluster.local slave 409edf623943d637f87733ed968068a4b4197566 0 1755338119109 19 connected
ae97d4725716ce86fdcc235826102cefb5616c9e 10.42.151.4:6379@16379,primary-2-0.primary.pqy.svc.cluster.local master - 0 1755338119000 18 connected 10923-16383
`
	sinfo, err := NewClusterNodesInfoFromLines(text)
	if err != nil {
		t.Error(err)
		return
	}
	list := sinfo.Nodes
	assert.True(t, sinfo != nil)
	assert.True(t, len(list) == 9)
	assert.True(t, sinfo.GetClusterSize() == 3)
	assert.True(t, sinfo.GetMasterCount() == 3)
	assert.True(t, sinfo.CheckAllMasterReady() == true)

	assert.True(t, list[0].NodeID == "409edf623943d637f87733ed968068a4b4197566")
	assert.True(t, list[0].Addr == "10.42.132.26:6379")
	assert.True(t, list[0].CPort == "16379")
	assert.True(t, list[0].HostName == "primary-1-1.primary.pqy.svc.cluster.local")
	assert.Equal(t, list[0].Flags, []string{"master"})
	assert.True(t, list[0].PrimaryNodeID == "")
	assert.True(t, list[0].PingSentUnixMs == "0")
	assert.True(t, list[0].PongRecvUnixMs == "1755338119109")
	assert.True(t, list[0].ConfigEpoch == "19")
	assert.True(t, list[0].LinkState == "connected")
	assert.True(t, list[0].SlotRange.Start == 5462)
	assert.True(t, list[0].SlotRange.End == 10922)

	assert.Equal(t, list[2].Flags, []string{"myself", "master"})

	assert.True(t, list[8].NodeID == "ae97d4725716ce86fdcc235826102cefb5616c9e")
	assert.True(t, list[8].Addr == "10.42.151.4:6379")
	assert.True(t, list[8].CPort == "16379")
	assert.True(t, list[8].HostName == "primary-2-0.primary.pqy.svc.cluster.local")
	assert.Equal(t, list[8].Flags, []string{"master"})
	assert.True(t, list[8].PrimaryNodeID == "")
	assert.True(t, list[8].PingSentUnixMs == "0")
	assert.True(t, list[8].PongRecvUnixMs == "1755338119000")
	assert.True(t, list[8].ConfigEpoch == "18")
	assert.True(t, list[8].LinkState == "connected")
	assert.True(t, list[8].SlotRange.Start == 10923)
	assert.True(t, list[8].SlotRange.End == 16383)
}
