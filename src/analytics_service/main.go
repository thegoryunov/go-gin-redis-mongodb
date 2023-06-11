package main

const (
	databaseName   = "blog"
	collectionName = "posts"
)

var (
	mongo_host = os.Getenv("MONGO2_HOST")
	mongo_port = os.Getenv("MONGO2_PORT")
	mongo_uri  = fmt.Sprintf("mongodb://%s:%s", mongo_host, mongo_port)
)

/*
func getAnalyticsDataByTitle(ctx *gin.Context, mongoClient *mongo.Client, title string) (bson.D, error) {
	coll := mongoClient.Database(databaseName).Collection(collectionName)
	var result bson.D
	err := coll.FindOne(ctx, bson.D{{"title", title}}).Decode(&result)
	if err != nil {
		log.Error().Err(err).Msg("error occured while fetching posts from posts mongo")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get analytics data"})
		return nil, err
	}
	return result, err
}
*/

func main() {

}
