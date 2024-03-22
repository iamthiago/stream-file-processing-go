package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"slices"
	"sort"
	"strconv"
	"strings"
	"fileservice-go/internal/fileservice/client"
	"fileservice-go/pkg/models"
)

const (
	IsCsvCompressed = true
	DateLayout      = "20060102-150405" // use the following layout yyyyMMDD-HHmmss
)

type Service struct {
	gcStore *client.GCStore
}

func NewFileService(gcStore *client.GCStore) *Service {
	return &Service{gcStore: gcStore}
}

func (s *Service) GetCsvFiles(bucket string, prefix string, ctx context.Context) []models.FileMetadata {
	var filesMetadata []models.FileMetadata
	var doneFiles []string
	var csvFilesFromOriginBucket []models.FileMetadata

	originIterator := s.gcStore.ListObjects(bucket, prefix, ctx)
	for {
		attrs, err := originIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("File in the origin Bucket: %v\n", attrs.Name)

		if strings.HasSuffix(attrs.Name, ".done") || (strings.HasSuffix(attrs.Name, ".gz") && attrs.Size > 0) {
			// add file to array
			filesMetadata = append(filesMetadata, models.FileMetadata{
				Name:    attrs.Name,
				Size:    attrs.Size,
				Updated: attrs.Updated,
			})

			// add done files without suffix to array
			var doneF, isValid = strings.CutSuffix(attrs.Name, ".done")
			if isValid {
				doneFiles = append(doneFiles, doneF)
			}
		}
	}

	// if .csv.gz file are in the .done file too, then add it to array
	for _, f := range filesMetadata {
		var gzFile, isValid = strings.CutSuffix(f.Name, ".csv.gz")
		if isValid && slices.Contains(doneFiles, gzFile) {
			csvFilesFromOriginBucket = append(csvFilesFromOriginBucket, f)
		}
	}

	return csvFilesFromOriginBucket
}

func (s *Service) GetTimestampFromLatestUploadedFiles(bucket string, prefix string, ctx context.Context) []string {
	var updatedTimeFromDestinationBucket []string
	destinationIterator := s.gcStore.ListObjects(bucket, prefix, ctx)
	for {
		attrs, err := destinationIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("File in the destination Bucket: %v\n", attrs.Name)

		updatedTimeFromDestinationBucket = append(updatedTimeFromDestinationBucket, attrs.Updated.String())
	}

	return updatedTimeFromDestinationBucket
}

func (s *Service) GetFilesToBeProcessed(csvFilesFromOriginBucket []models.FileMetadata, updatedTimeFromDestinationBucket []string) []models.FileMetadata {
	var filesToBeProcessed []models.FileMetadata
	if len(updatedTimeFromDestinationBucket) > 0 {
		for _, f1 := range updatedTimeFromDestinationBucket {
			log.Println(f1)
		}

		sort.Slice(updatedTimeFromDestinationBucket, func(i, j int) bool {
			return updatedTimeFromDestinationBucket[i] > updatedTimeFromDestinationBucket[j]
		})

		var latestUploadedFile = updatedTimeFromDestinationBucket[0]
		for _, f := range csvFilesFromOriginBucket {
			if f.Updated.String() > latestUploadedFile {
				filesToBeProcessed = append(filesToBeProcessed, f)
			}
		}
		log.Printf("Found CSV file for type CNAME on gcs, sending only latest %v files for processing\n", len(filesToBeProcessed))

	} else {
		log.Printf("No csv file on gcs of type CNAME, sending all new files for processing")
		filesToBeProcessed = append(filesToBeProcessed, csvFilesFromOriginBucket...)
	}

	return filesToBeProcessed
}

func (s *Service) ProcessCsvFile(oBucket string, dBucket string, dPrefix string, f models.FileMetadata, ctx context.Context) {
	headers := []string{"phoneNumber", "fullName", "firstName", "lastName", "address", "city", "area", "zipCode", "country",
		"jobTitle", "email", "companyName", "website", "longitude", "latitude", "subSource", "action", "categoryName",
		"isBusiness"}

	log.Printf("Processing file: %v from GCS Bucket (%v bytes)...\n", f.Name, f.Size)

	// get the file from the bucket and create a file reader
	obj := s.gcStore.Get(oBucket, f.Name, IsCsvCompressed)
	rdr, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Close()

	// gunzip it
	gzr, err := gzip.NewReader(rdr)
	if err != nil {
		log.Fatal(err)
	}
	defer gzr.Close()

	csvReader := csv.NewReader(gzr)

	// define a name and create a storage writer
	format := f.Updated.Format(DateLayout)
	var key = dPrefix + format + ".csv"
	wc := s.gcStore.Writer(dBucket, key, ctx)

	// create a csv writer in memory
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)

	// write the headers as first line in the csv
	writer.Write(headers)
	if _, err := io.Copy(wc, buf); err != nil {
		log.Fatal("io.Copy: %w", err)
	}
	writer.Flush()

	// as we iterate over the csv, write data to the new csv
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// transform
		// action,phone_number,first_name,last_name,category_name,is_business
		var data []string
		var action string
		var phoneNumber string
		var fullName string

		switch nrOfColumns := len(row); {
		case nrOfColumns == 5:
			action = "A"
			phoneNumber = row[0]
			var firstName = strings.TrimSpace(row[1])
			var lastName = strings.TrimSpace(row[2])
			var categoryName = row[3]
			var isBusiness = row[4]
			fullName = strings.TrimSpace(strings.Join([]string{firstName, lastName}, " "))
			data = []string{phoneNumber, fullName, firstName, lastName, "", "", "", "", "", "", "", "", "", "", "", "", action, categoryName, isBusiness}

		case nrOfColumns == 6:
			action = row[0]
			phoneNumber = row[1]
			var firstName = strings.TrimSpace(row[2])
			var lastName = strings.TrimSpace(row[3])
			var categoryName = row[4]
			var isBusiness = row[5]
			fullName = strings.TrimSpace(strings.Join([]string{firstName, lastName}, " "))
			data = []string{phoneNumber, fullName, firstName, lastName, "", "", "", "", "", "", "", "", "", "", "", "", action, categoryName, isBusiness}

		default:
			log.Fatalf("Number of columns %v, does not match", nrOfColumns)
		}

		// validation
		_, err = strconv.ParseInt(phoneNumber, 10, 64)
		if err == nil && len(fullName) > 0 {
			// write data to the new csv
			writer.Write(data)
			if _, err := io.Copy(wc, buf); err != nil {
				log.Fatal("io.Copy: %w", err)
			}
			writer.Flush()
		}
	}

	// close storage writer
	if err := wc.Close(); err != nil {
		log.Fatal("Writer.Close: %w", err)
	}

	log.Printf("File uploaded successfully")
	return
}
