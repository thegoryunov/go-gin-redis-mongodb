package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"gopkg.in/yaml.v3"
	"net/http"
	"os"
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

type BlogPost struct {
	Title  string `json:"title"`
	Author string `json:"author"`
	Body   string `json:"body"`
}

type Doc struct {
	Data bson.D `json:"data"`
}

type Docs struct {
	Data []bson.M `json:"data"`
}

var (
	analytics_service_host = "localhost"
	analytics_service_port = "8081"
	blog_service_host      = "localhost"
	blog_service_port      = "8082"
)

func getPost(ctx *gin.Context) {
	title := ctx.Param("title")
	address := fmt.Sprintf("http://%s:%s/posts/%s", blog_service_host, blog_service_port, title)
	resp, err := http.Get(address)
	if err != nil {
		log.Error().Err(err).Msg("error occured while fetching posts from posts service")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Get posts failed"})
		return
	}
	defer resp.Body.Close()
	val := &Doc{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(val)
	if err != nil {
		log.Error().Err(err).Msg("error occured while decoding response into Doc object")
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Get posts failed"})
		return
	}
	ctx.JSON(http.StatusOK, val)
}

func getAllPosts(ctx *gin.Context) {
	address := fmt.Sprintf("http://%s:%s/posts", blog_service_host, blog_service_port)
	resp, err := http.Get(address)
	if err != nil {
		log.Error().Err(err).Msg("error occured while fetching posts from posts service")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Get posts failed"})
		return
	}
	defer resp.Body.Close()
	val := &Docs{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(val)
	if err != nil {
		log.Error().Err(err).Msg("error occured while decoding response into Doc object")
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Get posts failed"})
		return
	}
	ctx.JSON(http.StatusOK, val)
}

func index(ctx *gin.Context) {
	ctx.HTML(http.StatusOK, "index.html", gin.H{})
}

func newPost(ctx *gin.Context, t string, a string, b string) {
	ctx.HTML(http.StatusOK, "post.html", gin.H{
		"title":  t,
		"author": a,
		"body":   b,
	})
}

func getPostViews(ctx *gin.Context) {
	title := ctx.Param("title")
	address := fmt.Sprintf("http://%s:%s/views/%s", analytics_service_host, analytics_service_port, title)
	fmt.Println(address)
	resp, err := http.Get(address)
	if err != nil {
		log.Error().Err(err).Msg("error occured while fetching views from views service")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Get views failed 1"})
		return
	}
	defer resp.Body.Close()
	val := &Doc{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(val)
	if err != nil {
		log.Error().Err(err).Msg("error occured while decoding response into Doc object")
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Get views failed 2"})
		return
	}
	ctx.JSON(http.StatusOK, val)
}

func getAllViews(ctx *gin.Context) {
	address := fmt.Sprintf("http://%s:%s/views", analytics_service_host, analytics_service_port)
	resp, err := http.Get(address)
	if err != nil {
		log.Error().Err(err).Msg("error occured while fetching views from views service")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Get views failed 3"})
		return
	}
	defer resp.Body.Close()
	val := &Docs{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(val)
	if err != nil {
		log.Error().Err(err).Msg("error occured while decoding response into Doc object")
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Get views failed 4"})
		return
	}
	ctx.JSON(http.StatusOK, val)
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
		//log.Error().Err(err).Msg("decode response")
	}

	opt, err := redis.ParseURL(config.Redis.Uri)
	if err != nil {
		panic(err)
	}
	rdb := redis.NewClient(opt)

	router := gin.Default()
	router.LoadHTMLGlob("templates/*.html")
	router.GET("/", index)
	router.POST("/posts", func(ctx *gin.Context) {
		title := ctx.PostForm("title")
		author := ctx.PostForm("author")
		body := ctx.PostForm("body")
		new_post := BlogPost{Title: title, Author: author, Body: body}
		payload, err := json.Marshal(new_post)
		if err != nil {
			log.Error().Err(err).Msg("error occured while decoding response into Doc object")
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Upload failed1"})
			return
		}
		fmt.Println(new_post)
		fmt.Println("==1==")
		err = json.Unmarshal(payload, &new_post)
		fmt.Println(payload)
		fmt.Println("==2==")
		if err != nil {
			log.Error().Err(err).Msg("error occured while decoding response into Doc object")
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Upload failed2"})
			return
		}
		fmt.Println(payload)
		fmt.Println("==3==")
		fmt.Println(string(payload))
		if err := rdb.RPush(ctx, "queue:new-post", payload).Err(); err != nil {
			log.Error().Err(err).Msg("error occured while decoding response into Doc object")
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Upload failed3"})
		}

		newPost(ctx, title, author, body)
	})
	router.GET("/posts/:title", getPost)
	router.GET("/posts", getAllPosts)
	router.GET("/views/:title", getPostViews)
	router.GET("/views", getAllViews)
	router.Run(":8080")

}
