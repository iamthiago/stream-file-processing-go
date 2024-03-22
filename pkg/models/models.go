package models

import "time"

type FileMetadata struct {
	Name    string
	Size    int64
	Updated time.Time
}
