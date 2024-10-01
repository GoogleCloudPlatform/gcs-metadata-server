package model

type Directory struct {
	Bucket string `json:"bucket" db:"bucket"`
	Name   string `json:"name" db:"name"`
	Size   int64  `json:"size" db:"size"`
	Count  int64  `json:"count" db:"count"`
}
