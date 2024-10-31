package repo

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

type BatchWriter struct {
	db            *Database
	inputChan     chan *model.Metadata
	batchSize     int
	metadataRepo  MetadataRepository
	directoryRepo DirectoryRepository
	flushInterval time.Duration
	wg            sync.WaitGroup
}

type BatchWriterRepository interface {
	Start(ctx context.Context)
	Stop()
	Add(metadata *model.Metadata)
	run(ctx context.Context)
}

func NewBatchWriter(db *Database, batchSize int, flushInterval time.Duration) BatchWriterRepository {
	return &BatchWriter{
		db:            db,
		metadataRepo:  NewMetadataRepository(db),
		directoryRepo: NewDirectoryRepository(db),
		inputChan:     make(chan *model.Metadata),
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

func (b *BatchWriter) Start(ctx context.Context) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.run(ctx)
	}()
}

func (b *BatchWriter) Stop() {
	close(b.inputChan)
	b.wg.Wait()
}

func (b *BatchWriter) Add(metadata *model.Metadata) {
	b.inputChan <- metadata
}

func (b *BatchWriter) run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]*model.Metadata, 0, b.batchSize)

	for {
		select {
		case <-ctx.Done():
			b.flush(batch)
			return
		case metadata, ok := <-b.inputChan:
			if !ok {
				b.flush(batch)
				return
			}
			batch = append(batch, metadata)
			if len(batch) >= b.batchSize {
				b.flush(batch)
				batch = make([]*model.Metadata, 0, b.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = make([]*model.Metadata, 0, b.batchSize)
			}
		}
	}
}

func (b *BatchWriter) flush(batch []*model.Metadata) {
	if len(batch) == 0 {
		return
	}

	tx, err := b.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}

	for _, metadata := range batch {
		if err := b.metadataRepo.InsertTx(tx, metadata); err != nil {
			log.Printf("Error inserting metadata, %s: %v", metadata.Name, err)
			if err := tx.Rollback(); err != nil {
				log.Printf("Error rolling back transaction: %v", err)
			}
			return
		}

		if err := b.directoryRepo.UpsertParentDirsTx(tx, StorageClass(metadata.StorageClass), metadata.Bucket, metadata.Name, metadata.Size, 1); err != nil {
			log.Printf("Error upserting directories: %v", err)
			if err := tx.Rollback(); err != nil {
				log.Printf("Error rolling back transaction: %v", err)
			}
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
	}
}
