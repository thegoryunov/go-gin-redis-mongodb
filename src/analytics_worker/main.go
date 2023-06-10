package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	databaseName     = "blog"
	collectionName   = "views"
	resultKeyIndex   = 0
	resultValueIndex = 1
)

var (
	mongo_host = os.Getenv("MONGO2_HOST")
	mongo_port = os.Getenv("MONGO2_PORT")
	redis_host = os.Getenv("REDIS_HOST")
	redis_port = os.Getenv("REDIS_PORT")
	mongo_uri  = fmt.Sprintf("mongodb://%s:%s", mongo_host, mongo_port)
	redis_uri  = fmt.Sprintf("redis://%s:%s/0", redis_host, redis_port)
)

type AnalyticsData struct {
	Title string
	Views int
}

func getDoc(mongoClient *mongo.Client, title string) (AnalytcisData, error) {
	coll := mongoClient.Database(databaseName).Collection(collectionName)
	var result AnalytcisData
	err := coll.FindOne(context.TODO(), bson.D{{"title", title}}).Decode(&result)
	if err != nil {
		log.Error().Err(err).Msg("Error while getting doc from mongo")
		return result, err
	}
	return result, err
}

func insertDoc(mongoClient *mongo.Client, title string) (*mongo.InertOneResult, error) {
	coll := mongoClient.Database(databaseName).Collection(collectionName)
	data := AnalytcisData{Title: title, Views: 1}
	result, err := coll.InsertOne(context.TODO(), data)

	if err != nil {
		log.Error().Err(err).Msg("Error while inserting doc to mongo")
		return result, err
	}
	return result, err
}

func updateAnalytics(client *mongo.Client, title string) {

	existingDoc, err := getDoc(MongoClient, title)
	if err != nil {
		log.Error().Err(err).Msg("Error while getting doc")
	}
	if existingDoc.Title == "" {
		insertDoc(MongoClient, title)
	} else {
		views := existingDoc.Views + 1
		coll := MongoClient.Database("blog").Collection("views")
		_, err := coll.UpdateOne(
			context.TODO(),
			bson.M{"title": existingDoc.Title},
			bson.D{
				{"$set", bson.D{{"views", views}}},
			},
		)
		if err != nil {
			log.Error().Err(err).Msg("Error while updating views")
		}
	}
}

func main() {
	ctx := context.Background()
	MongoClient, err := mongo.NewClient(options.Client().ApplyURI(mongo_uri))
	if err != nil {
		log.Error().Err(err).Msg("Error while creating mongo client")
	}
	err = MongoClient.Connect(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Error while connecting to mongo")
	}
	defer MongoClient.Disconnect(ctx)
	opt, err := redis.ParseURL(redis_uri)
	if err != nil {
		log.Error().Err(err).Msg("Error while parsing redis url")
	}
	rdb := redis.NewClient(opt)
	for {
		result, err := rdb.BLPop(ctx, 0, "queue:blog-view").Result()
		if err != nil {
			log.Error().Err(err).Msg("Error while getting data from redis")
			continue
		}
		updateAnalytics(MongoClient, result[1])
		fmt.Println(result[resultKeyIndex], result[resultValueIndex])
	}
}
