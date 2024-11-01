package seeder

import (
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type testObjectIterator struct {
	items []*storage.ObjectAttrs
	index int
	err   error
}

func (t *testObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if t.err != nil {
		return nil, t.err
	}

	if t.index >= len(t.items) {
		return nil, iterator.Done
	}

	obj := t.items[t.index]
	t.index++
	return obj, nil
}
