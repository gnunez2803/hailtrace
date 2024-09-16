package main

import (
	"context"
	"fmt"
	"os"
	"weather-etl/internal/storm"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	process, err := storm.InitProcess()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	process.Start(ctx)
}
