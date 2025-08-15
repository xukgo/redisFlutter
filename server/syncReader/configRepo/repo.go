package configRepo

import (
	"encoding/json"
	"log/slog"
	"sync"
)

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

func (c *Repo) ReadCall(cb func(*ServerConfig)) {
	c.locker.Lock()
	defer c.locker.Unlock()
	cb(c.appConfig)
}
