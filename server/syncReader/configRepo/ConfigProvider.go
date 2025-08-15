package configRepo

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

type IConfigProvider interface {
	ReadCall(func(*ServerConfig))
}
