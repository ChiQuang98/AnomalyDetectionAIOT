package setting

import (
	"encoding/json"
	"io/ioutil"
)

type GlogConfigs struct {
	LogDir  string
	MaxSize uint64
	V       int
}

func GetGlogConfig() *GlogConfigs {
	return settings.GlogConfig
}

type Settings struct {
	GlogConfig     *GlogConfigs
	RestfulApiPort int
	RestfulApiHost string
	KafkaInfo      *KafkaInfo
	NatsInfo       *NatsInfo
	KSQLInfo       *KSQLInfo
}
type KafkaInfo struct {
	ServerAddress1 string
	ServerAddress2 string
	ServerAddress3 string
}
type NatsInfo struct {
	Host string
}
type KSQLInfo struct {
	Host string
}

var settings Settings = Settings{}

func init() {
	content, err := ioutil.ReadFile("setting.json")
	if err != nil {
		panic(err)
	}
	settings = Settings{}
	jsonErr := json.Unmarshal(content, &settings)
	if jsonErr != nil {
		panic(jsonErr)
	}
}
func GetRestfulApiPort() int {
	return settings.RestfulApiPort
}
func GetRestfulApiHost() string {
	return settings.RestfulApiHost
}
func GetKafkaInfo() *KafkaInfo {
	return settings.KafkaInfo
}
func GetNatsInfo() *NatsInfo {
	return settings.NatsInfo
}
func GetKSQLInfo() *KSQLInfo {
	return settings.KSQLInfo
}
