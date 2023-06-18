package main

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Mongo struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
		Uri      string `yaml:"uri"`
	} `yaml:"mongo"`
	Redis struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
		Uri      string `yaml:"uri"`
	} `yaml:"redis"`
}

const (
	databaseName   = "blog"
	collectionName = "posts"
)

func getPost(ctx *gin.Context, mongoClient *mongo.Client, title string) (bson.D, error) {
	coll := mongoClient.Database(databaseName).Collection(collectionName)
	var result bson.D
	err := coll.FindOne(ctx, bson.D{{"title", title}}).Decode(&result)
	if err != nil {
		log.Error().Err(err).Msg("error occured while fetching posts from posts mongo")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Get post failed"})
		return nil, err
	}
	Publish(ctx, title)
	return result, err
}

func Publish(ctx *gin.Context, payload string) {

	//TODO: move to context?

	// open config file
	file, err := os.Open("../../config.yml")
	if err != nil {
		fmt.Println("error opening file:", err)
	}
	defer file.Close()

	// read config file
	var config Config
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}

	opt, err := redis.ParseURL(config.Redis.Uri)
	if err != nil {
		log.Error().Err(err).Msg("error occured while connecting to redis")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Analytics error"})
		return
	}
	rdb := redis.NewClient(opt)
	if err := rdb.RPush(ctx, "queue:blog-view", payload).Err(); err != nil {
		log.Error().Err(err).Msg("error occured while publishing to redis")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Analytics error"})
		return
	}
}

func main() {

	// open config file
	file, err := os.Open("../../config.yml")
	if err != nil {
		fmt.Println("error opening file:", err)
	}
	defer file.Close()

	// read config file
	var config Config
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}

	mongoClient, err := mongo.NewClient(options.Client().ApplyURI(config.Mongo.Uri))
	if err != nil {
		log.Error().Err(err).Msg("error occured while connecting to mongo")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = mongoClient.Connect(ctx)
	if err != nil {
		log.Error().Err(err).Msg("error occured while connecting to mongo")
	}
	defer mongoClient.Disconnect(ctx)
	router := gin.Default()

	router.GET("/posts/:title", func(ctx *gin.Context) {
		title := ctx.Param("title")
		result, err := getPost(ctx, mongoClient, title)
		if err != nil {
			log.Error().Err(err).Msg("error occured while fetching post from mongo")
		}
		ctx.JSON(http.StatusOK, gin.H{
			"Data": result,
		})
	})
	router.Run(":8082")
}
