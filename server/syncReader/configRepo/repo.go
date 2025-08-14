package configRepo

import (
	"encoding/json"
	"log/slog"
	"sync"
)

type RedisServerConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
}
type ServerConfig struct {
	LogLevel       string            `json:"logLevel"`
	TargetInstance string            `json:"targetInstance"`
	TargetEndpoint string            `json:"targetEndpoint"`
	SaveDataDir    string            `json:"dataDir"`
	RedisServer    RedisServerConfig `json:"redisServer"`
}

type Repo struct {
	locker    *sync.RWMutex
	appConfig *ServerConfig
}

var singleton = newRepo()

func newRepo() *Repo {
	r := new(Repo)
	r.locker = new(sync.RWMutex)
	return r
}
func GetSingleton() *Repo {
	return singleton
}

func (c *Repo) UpdateFromContent(content []byte) {
	c.locker.Lock()
	defer c.locker.Unlock()

	localAppConfig := new(ServerConfig)
	err := json.Unmarshal(content, localAppConfig)
	if err != nil {
		slog.Error("syncReader config json unmarshal err", slog.String("err", err.Error()))
		return
	}
	c.appConfig = localAppConfig
	slog.Info("config json update done")
}
