package main

import (
	"cloud.google.com/go/storage"
	"context"
	"fileservice-go/internal/fileservice/client"
	"fileservice-go/internal/fileservice/service"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"sort"
)

func main() {
	viper.SetConfigName("conf")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./conf/")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	var config = viper.Sub("file-service-importer")
	var originBucket = config.GetString("gcs.origin.bucket")
	var originPrefix = config.GetString("gcs.origin.prefix")
	var destinationBucket = config.GetString("gcs.destination.bucket")
	var destinationPrefix = config.GetString("gcs.destination.prefix")

	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer gcsClient.Close()

	gcStore := client.NewGCStore(gcsClient)
	fileService := service.NewFileService(gcStore)

	var csvFilesFromOriginBucket = fileService.GetCsvFiles(originBucket, originPrefix, ctx)
	var updatedTimeFromDestinationBucket = fileService.GetTimestampFromLatestUploadedFiles(destinationBucket, destinationPrefix, ctx)
	var filesToBeProcessed = fileService.GetFilesToBeProcessed(csvFilesFromOriginBucket, updatedTimeFromDestinationBucket)

	// process files
	if len(filesToBeProcessed) > 0 {
		// sort before processing
		sort.Slice(filesToBeProcessed, func(i, j int) bool {
			return filesToBeProcessed[i].Updated.Before(filesToBeProcessed[j].Updated)
		})

		for _, f := range filesToBeProcessed {
			fileService.ProcessCsvFile(originBucket, destinationBucket, destinationPrefix, f, ctx)
		}

		log.Printf("Job completed. Exiting...")
		return

	} else {
		log.Printf("Nothing to do")
		return
	}
}
