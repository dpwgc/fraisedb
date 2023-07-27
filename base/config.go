package base

import (
	"gopkg.in/yaml.v3"
	"os"
)

type ConfigModel struct {
	Node  NodeConfig  `yaml:"node" json:"node"`
	Store StoreConfig `yaml:"store" json:"store"`
}

type NodeConfig struct {
	First    bool   `yaml:"first" json:"first"`
	Addr     string `yaml:"addr" json:"addr"`
	TcpPort  int    `yaml:"tcpPort" json:"tcpPort"`
	HttpPort int    `yaml:"httpPort" json:"httpPort"`
}

type StoreConfig struct {
	Data string `yaml:"data" json:"data"`
	Log  string `yaml:"log" json:"log"`
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
