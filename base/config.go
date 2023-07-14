package base

import (
	"gopkg.in/yaml.v3"
	"os"
)

type ConfigModel struct {
	Node  NodeConfig  `yaml:"node"`
	Store StoreConfig `yaml:"store"`
}

type NodeConfig struct {
	First    bool   `yaml:"first"`
	Addr     string `yaml:"addr"`
	TcpPort  int    `yaml:"tcpPort"`
	HttpPort int    `yaml:"httpPort"`
}

type StoreConfig struct {
	Data string `yaml:"data"`
	Log  string `yaml:"log"`
}

// InitConfig 加载配置
func InitConfig() {
	localConfigBytes := loadConfigFile("./config.yaml")
	err := yaml.Unmarshal(localConfigBytes, &config)
	if err != nil {
		panic(err)
	}
}

func Config() ConfigModel {
	return config
}

// 读取本地配置文件
func loadConfigFile(path string) []byte {
	//加载本地配置
	configBytes, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return configBytes
}
