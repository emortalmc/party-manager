package config

import (
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	MongoDB  MongoDBConfig  `yaml:"mongo"`
	RabbitMQ RabbitMQConfig `yaml:"rabbitmq"`

	Port        uint16 `yaml:"port"`
	Development bool   `yaml:"development"`
}

type MongoDBConfig struct {
	URI string `yaml:"uri"`
}

type RabbitMQConfig struct {
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func LoadGlobalConfig() (config *Config, err error) {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	if err = viper.ReadInConfig(); err != nil {
		return nil, err
	}

	if err = viper.UnmarshalExact(&config); err != nil {
		return nil, err
	}

	return config, nil
}
