package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"weather-api/internal/weather"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	err := godotenv.Load()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// Create a new Gin router
	router := gin.Default()
	dbRepo, err := weather.NewMysqlRepository(os.Getenv("DATABASE_URL"))
	if err != nil {
		logger.Error("Unable to initialize DB connection.",
			zap.String("error", err.Error()))
		os.Exit(1)
	}
	stormRepo := weather.NewModelsRepo(dbRepo)
	process, err := weather.InitProcess(stormRepo)
	if err != nil {
		logger.Error("Unable to initialize Kafka connection.",
			zap.String("error", err.Error()))
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go process.Start(ctx)

	// Define a simple GET route
	router.GET("/storm", func(c *gin.Context) {
		dateStr := c.Query("date")

		location := c.Query("location")
		_, err := time.Parse("2006-01-02", dateStr)
		if dateStr == "" || err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "date format must be specified and be in the format YYYY-MM-DD",
			},
			)
			return
		}

		response, err := stormRepo.GetStorms(location, dateStr)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"Failure": err,
			})
			return
		}
		c.JSON(http.StatusOK, response)
	})
	// Start the server on port 8080
	router.Run(":8080")
}
