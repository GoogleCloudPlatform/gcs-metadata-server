package subscriber

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
)

type payload struct {
	Bucket       string
	Name         string
	Size         string
	StorageClass string
	Updated      time.Time
	Created      time.Time
}

type Susbcriber interface {
	Start(ctx context.Context) error
	consumeMessage(ctx context.Context, msg *pubsub.Message)
	handleFinalize(inMetadata *model.Metadata) error
	handleArchive(inMetadata *model.Metadata) error
	handleDelete(inMetadata *model.Metadata) error
}

type SubscriberService struct {
	client         *pubsub.Client
	subscriptionId string
	directoryRepo  repo.DirectoryRepository
	metadataRepo   repo.MetadataRepository
}

func NewSubscriberService(client *pubsub.Client, subscriptionId string, directoryRepo repo.DirectoryRepository, metadataRepo repo.MetadataRepository) *SubscriberService {
	return &SubscriberService{
		client,
		subscriptionId,
		directoryRepo,
		metadataRepo,
	}
}

func newMetadata(p payload) (*model.Metadata, error) {
	size, err := strconv.ParseInt(p.Size, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing size: %v", err.Error())
	}

	return &model.Metadata{
		Bucket:       p.Bucket,
		Name:         p.Name,
		Size:         size,
		StorageClass: p.StorageClass,
		Updated:      p.Updated,
		Created:      p.Created,
	}, nil
}

// Start initiates subscription process by listening to all messages at subscriptionId
func (s *SubscriberService) Start(ctx context.Context) error {
	sub := s.client.SubscriptionInProject(s.subscriptionId, s.client.Project())

	if err := sub.Receive(ctx, s.consumeMessage); err != nil {
		return fmt.Errorf("error receiving messages %w", err)
	}
	return nil
}

func nackLog(msg *pubsub.Message, err error) {
	log.Printf("Subscriber error: %v\n", err)
	msg.Nack() // TODO: Improve error handling by adding proper logging
}

// consumeMessage is a callback function for pubsub.Receive() which performs
// updates to database according to incoming metadata.
//
// Messages are expected to be unordered so the handling of incoming metadata has to
// be based on its update time and gracefully Nack()'d when necessary
func (s *SubscriberService) consumeMessage(ctx context.Context, msg *pubsub.Message) {
	var p payload
	if err := json.Unmarshal(msg.Data, &p); err != nil {
		nackLog(msg, err)
		return
	}

	inMetadata, err := newMetadata(p)
	if err != nil {
		nackLog(msg, err)
		return
	}

	_, isReplaced := msg.Attributes["overwrittenByGeneration"]
	eventType := msg.Attributes["eventType"]

	switch eventType {
	case storage.ObjectFinalizeEvent:
		if err := s.handleFinalize(inMetadata); err != nil {
			nackLog(msg, err)
			return
		}

	case storage.ObjectDeleteEvent:
		// Ignore replacement events
		if isReplaced {
			msg.Ack()
			return
		}

		if err := s.handleDelete(inMetadata); err != nil {
			nackLog(msg, err)
			return
		}
	case storage.ObjectArchiveEvent:
		// Ignore replacement events
		if isReplaced {
			msg.Ack()
			return
		}

		if err := s.handleArchive(inMetadata); err != nil {
			nackLog(msg, err)
			return
		}
	default:
		defaultErr := fmt.Errorf("unknown event type: %s", eventType)
		nackLog(msg, defaultErr)
	}

	msg.Ack()
}

// handleFinalize takes incoming metadata and determines to insert or update
// based on if metadata already exists and is newer
func (s *SubscriberService) handleFinalize(inMetadata *model.Metadata) error {
	existingMetadata, err := s.metadataRepo.Get(inMetadata.Bucket, inMetadata.Name)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("error getting existing metadata: %w", err)
	}

	// Check if incoming metadata is necessary to handle
	if existingMetadata != nil {
		if existingMetadata.Updated.After(inMetadata.Updated) {
			return nil
		}

		if existingMetadata.StorageClass != inMetadata.StorageClass {
			return s.handleArchive(inMetadata)
		}
	}

	// Insert if metadata does not exist
	if existingMetadata == nil {
		if err := s.metadataRepo.Insert(inMetadata); err != nil {
			return fmt.Errorf("error inserting metadata: %w", err)
		}
		if err := s.directoryRepo.UpsertParentDirs(repo.StorageClass(inMetadata.StorageClass), inMetadata.Bucket, inMetadata.Name, inMetadata.Size, 1); err != nil {
			return fmt.Errorf("error upserting parent directories: %w", err)
		}
	} else {
		// Otherwise, update metadata
		if err := s.metadataRepo.Update(inMetadata.Bucket, inMetadata.Name, inMetadata.StorageClass, inMetadata.Size, inMetadata.Updated); err != nil {
			return fmt.Errorf("error updating metadata: %w", err)
		}

		sizeDiff := inMetadata.Size - existingMetadata.Size
		if err := s.directoryRepo.UpsertParentDirs(repo.StorageClass(inMetadata.StorageClass), inMetadata.Bucket, inMetadata.Name, sizeDiff, 0); err != nil {
			return fmt.Errorf("error upserting parent directories: %w", err)
		}
	}
	return nil
}

// handleArchive takes incoming metadata and updates parent directories to new storage class
func (s *SubscriberService) handleArchive(inMetadata *model.Metadata) error {
	existingMetadata, err := s.metadataRepo.Get(inMetadata.Bucket, inMetadata.Name)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("error getting existing metadata: %w", err)
	}

	// Check if incoming metadata is necessary to handle
	if existingMetadata != nil {
		if existingMetadata.Updated.After(inMetadata.Updated) {
			return nil // skip, already in most recent update
		}

		if existingMetadata.StorageClass == inMetadata.StorageClass {
			return nil // skip, already in correct StorageClass
		}
	} else {
		// if metadata does not exist, it is a normal insert
		return s.handleFinalize(inMetadata)
	}

	if err := s.metadataRepo.Update(inMetadata.Bucket, inMetadata.Name, inMetadata.StorageClass,
		inMetadata.Size, inMetadata.Updated); err != nil {
		return fmt.Errorf("error updating metadata: %w", err)
	}

	if err := s.directoryRepo.UpsertArchiveParentDirs(repo.StorageClass(existingMetadata.StorageClass),
		repo.StorageClass(inMetadata.StorageClass), inMetadata.Bucket, inMetadata.Name, inMetadata.Size); err != nil {
		return fmt.Errorf("error upserting parent directories: %w", err)
	}

	return nil
}

// handleDelete tries to delete incoming metadata inMetadata.
// Returns error if metadata does not exist
func (s *SubscriberService) handleDelete(inMetadata *model.Metadata) error {
	// Check if metadata exists
	existingMetadata, err := s.metadataRepo.Get(inMetadata.Bucket, inMetadata.Name)
	if err != nil {
		return err
	}

	// Skip if existing metadata is newer
	if existingMetadata.Updated.After(inMetadata.Updated) {
		return nil
	}

	if err := s.metadataRepo.Delete(inMetadata.Bucket, inMetadata.Name); err != nil {
		return err
	}

	return s.directoryRepo.UpsertParentDirs(repo.StorageClass(inMetadata.StorageClass), inMetadata.Bucket,
		inMetadata.Name, -inMetadata.Size, -1)
}
