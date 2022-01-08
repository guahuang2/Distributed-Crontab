package master

import (
	"encoding/json"
	"io/ioutil"
)

// application configuration
type Config struct {
	ApiPort               int      `json:"apiPort"`
	ApiReadTimeout        int      `json:"apiReadTimeout"`
	ApiWriteTimeout       int      `json:"apiWriteTimeout"`
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	WebRoot               string   `json:"webroot"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

var (
	// singleton
	G_config *Config
)

// load configuation
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 1, read the configuration file
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2, deserialize json
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	// 3, assign to singleton
	G_config = &conf

	return
}
