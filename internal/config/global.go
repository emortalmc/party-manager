package config

import (
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	MongoDB  MongoDBConfig
	RabbitMQ RabbitMQConfig

	Port        uint16
	Development bool
}

type MongoDBConfig struct {
	URI string
}

type RabbitMQConfig struct {
	Host     string
	Username string
	Password string
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
