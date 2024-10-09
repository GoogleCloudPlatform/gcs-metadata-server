package repo

import "errors"

type StorageClass string
type Location string

const (
	StorageStandard StorageClass = "STANDARD"
	StorageNearline StorageClass = "NEARLINE"
	StorageColdline StorageClass = "COLDLINE"
	StorageArchive  StorageClass = "ARCHIVE"

	LocationUS   Location = "US"
	LocationASIA Location = "ASIA"
	LocationEU   Location = "EU"
	LocationCA   Location = "CA"
	LocationAU   Location = "AU"
	LocationIN   Location = "IN"
)

const bytesPerGB = 1024 * 1024 * 1024

// LocationPricing holds a general pricing per location
// based on the most expensive region for each location
//
// Pricing is sourced from https://cloud.google.com/storage/pricing
var locationPricing = map[Location]map[StorageClass]float64{
	LocationUS: { // us-west4
		StorageStandard: 0.0230,
		StorageNearline: 0.0160,
		StorageColdline: 0.0070,
		StorageArchive:  0.0025,
	},
	LocationASIA: { // asia-east2
		StorageStandard: 0.0230,
		StorageNearline: 0.0160,
		StorageColdline: 0.0070,
		StorageArchive:  0.0025,
	},
	LocationEU: { // europe-west6
		StorageStandard: 0.0250,
		StorageNearline: 0.0100,
		StorageColdline: 0.0070,
		StorageArchive:  0.0025,
	},
	LocationCA: { // nothamerica-northeast1
		StorageStandard: 0.0230,
		StorageNearline: 0.0130,
		StorageColdline: 0.0070,
		StorageArchive:  0.0025,
	},
	LocationAU: { // australia-southeast1
		StorageStandard: 0.0230,
		StorageNearline: 0.0160,
		StorageColdline: 0.0060,
		StorageArchive:  0.0025,
	},
	LocationIN: { // asia-south2
		StorageStandard: 0.0230,
		StorageNearline: 0.0160,
		StorageColdline: 0.0060,
		StorageArchive:  0.0025,
	},
}

// GetPrice returns the price for a given storage class in a specific location.
func getPrice(location Location, storageClass StorageClass, size int64) (float64, error) {
	price, ok := locationPricing[location][storageClass]
	if !ok {
		return 0, errors.New("invalid location or storage class")
	}

	sizeGb := float64(size / bytesPerGB)
	return price * sizeGb, nil
}

func getDirectoryTotalCost(location Location, sizeStandard, sizeNearline, sizeColdline, sizeArchive int64) (float64, error) {
	var totalCost float64 = 0

	classes := []StorageClass{StorageStandard, StorageNearline, StorageColdline, StorageArchive}
	sizes := []int64{sizeStandard, sizeNearline, sizeColdline, sizeArchive}

	for i, size := range sizes {
		cost, err := getPrice(location, classes[i], size)
		if err != nil {
			return 0, err
		}
		totalCost += cost
	}
	return totalCost, nil
}
