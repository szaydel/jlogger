package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"os"

// 	"github.com/go-redis/redis/v7"
// )

// // RedisConfig is a struct representing Redis server configuration.
// type RedisConfig struct {
// 	Server   string `json:"server"`
// 	Port     uint16 `json:"port"`
// 	DB       int    `json:"db"`
// 	Password string `json:"password"`
// }

// // Addr emits a host:port value passed as Addr parameter to redis.Options.
// func (rc *RedisConfig) Addr() string {
// 	return fmt.Sprintf("%s:%d", rc.Server, rc.Port)
// }

// // ToRedisOptions builds a *redis.Options struct for convenient interaction
// // with Redis client.
// func (rc *RedisConfig) ToRedisOptions() *redis.Options {
// 	return &redis.Options{
// 		Addr:     rc.Addr(),
// 		Password: rc.Password,
// 		DB:       rc.DB,
// 	}
// }

// // NewRedisConfig reads a JSON configuration file and un-marshals contents of
// // this file into a *RedisConfig struct.
// func NewRedisConfig(name string) *RedisConfig {
// 	c := &RedisConfig{}
// 	var file *os.File
// 	var err error
// 	if file, err = os.Open(name); err != nil {
// 		return c
// 	}
// 	defer file.Close()
// 	d := json.NewDecoder(file)
// 	d.Decode(c)
// 	return c
// }
