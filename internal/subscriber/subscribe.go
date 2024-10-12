package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
)

type EventType string

const (
	EventTypeFinalize EventType = "OBJECT_FINALIZE"
	EventTypeDelete   EventType = "OBJECT_DELETE"
)

type payload struct {
	Bucket       string
	Name         string
	Size         string
	StorageClass string
	Updated      time.Time
	Created      time.Time
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

// Start initiates subscription process by listening to all messages at subscriptionId
func (s *SubscriberService) Start(ctx context.Context) error {
	sub := s.client.SubscriptionInProject(s.subscriptionId, s.client.Project())

	if err := sub.Receive(ctx, s.consumeMessage); err != nil {
		return fmt.Errorf("error receiving messages %w", err)
	}
	return nil
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
		Created:      p.Created,
		Updated:      p.Updated,
	}, nil
}

func (s *SubscriberService) consumeMessage(ctx context.Context, msg *pubsub.Message) {
	// Parse payload
	var p payload
	if err := json.Unmarshal([]byte(msg.Data), &p); err != nil {
		log.Printf("Error unmarshalling message payload: %v\n", err)
		msg.Nack() // TODO: replace with proper error handling for improperly formatted messages
		return
	}

	inMetadata, err := newMetadata(p)
	if err != nil {
		log.Printf("Error parsing metadata: %v\n", err)
		msg.Nack() // TODO: replace with proper error handling for improperly formatted messages
		return
	}
	_ = inMetadata

	eventType := EventType(msg.Attributes["eventType"])
	_, isUpdate := msg.Attributes["overwroteGeneration"] // 'overwroteGeneration' is only included in update events
	_ = isUpdate

	// Handle update/insert events
	if eventType == EventTypeFinalize {
		// Check if message is newer than existing metadata

		if isUpdate {
			// Handle update
		} else {
			// Handle insert
		}
	}

	// Handle delete event
	if eventType == EventTypeDelete {
	}

	msg.Ack()
}
