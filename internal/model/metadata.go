package model

import "time"

type Metadata struct {
	Bucket       string    `json:"bucket" db:"bucket"`
	Name         string    `json:"name" db:"name"`
	Size         int64     `json:"size" db:"size"`
	StorageClass string    `json:"storage_class" db:"storage_class"`
	Created      time.Time `json:"created" db:"created"`
	Updated      time.Time `json:"updated" db:"updated"`
}
