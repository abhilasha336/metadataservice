package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

var (
	kafkaBrokers  = []string{"localhost:9092"}
	metadataStore = make(map[string]map[string]interface{})
)

func getKafkaConsumer() sarama.Consumer {
	consumer, err := sarama.NewConsumer(kafkaBrokers, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	return consumer
}

func fetchMetadataFromKafka() {
	consumer := getKafkaConsumer()
	defer consumer.Close()

	partitions, err := consumer.Partitions("metadata")
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition("metadata", partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %v", partition, err)
		}
		defer pc.Close()

		for msg := range pc.Messages() {
			var metadata map[string]interface{}
			if err := json.Unmarshal(msg.Value, &metadata); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				continue
			}

			username, ok := metadata["username"].(string)
			if !ok {
				log.Printf("Invalid metadata format: %v", metadata)
				continue
			}

			metadataStore[username] = metadata
			log.Printf("Metadata for user %s stored: %v", username, metadata)
		}
	}
}

func metadata(ctx *gin.Context) {
	username := ctx.Query("username")
	if username == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Username query parameter is required"})
		return
	}

	metadata, found := metadataStore[username]
	if !found {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found for the given username"})
		return
	}

	ctx.JSON(http.StatusOK, metadata)
}

func main() {
	go fetchMetadataFromKafka()

	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowHeaders = []string{"Authorization", "Content-Type"}

	r := gin.Default()
	r.Use(cors.New(corsConfig))

	r.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"hi": "hello"})
	})

	r.GET("/metadata", metadata)

	if err := r.Run(":8084"); err != nil {
		log.Fatal("Failed to run server: ", err)
		return
	}

	fmt.Println("Server started successfully on port: 8084")

	// Keep the main function running to allow the goroutine to continue
	select {}
}
