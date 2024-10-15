package model

type Directory struct {
	Bucket       string `json:"bucket" db:"bucket"`
	Name         string `json:"name" db:"name"`
	SizeStandard int64  `db:"size_standard"`
	SizeNearline int64  `db:"size_nearline"`
	SizeColdline int64  `db:"size_coldline"`
	SizeArchive  int64  `db:"size_archive"`
	Count        int64  `json:"count" db:"count"`
}
