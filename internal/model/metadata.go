package model

import "time"

type Metadata struct {
	Bucket       string    `json:"bucket" db:"bucket"`
	Name         string    `json:"name" db:"name"`
	Parent       string    `json:"parent" db:"parent"`
	StorageClass string    `json:"storageClass" db:"storage_class"`
	Size         int64     `json:"size" db:"size"`
	Count        int64     `json:"count" db:"count"`
	Cost         float64   `json:"cost" db:"cost"`
	Created      time.Time `json:"created" db:"created"`
	Updated      time.Time `json:"updated" db:"updated"`
}
