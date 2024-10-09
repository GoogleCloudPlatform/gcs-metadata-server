package model

type Summary struct {
	Path string `json:"path" db:"name"`
	Cost `json:"cost"`
	Size `json:"size"`
}

type Size struct {
	Standard int64 `json:"standard" db:"size_standard"`
	Nearline int64 `json:"nearline" db:"size_nearline"`
	Coldline int64 `json:"coldline" db:"size_coldline"`
	Archive  int64 `json:"archive" db:"size_archive"`
}

type Cost struct {
	Standard float64 `json:"standard"`
	Nearline float64 `json:"nearline"`
	Coldline float64 `json:"coldline"`
	Archive  float64 `json:"archive"`
}
