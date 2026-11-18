package engine

import (
	"errors"
	"fmt"
	"lamina/internal/blobs"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	mu           sync.RWMutex
	dir          string
	catalog      *catalog.Catalog
	store        *versions.Store
	indexes      *index.Registry
	blobStore    *blobs.Store
	wal          *journal.WAL
	sequence     uint64
	horizon      uint64
	transactions map[string]*Transaction
	closed       bool
	commits      uint64
	conflicts    uint64
	queries      uint64
}

func Open(dir string) (*DB, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	image, err := snapshot.Read(filepath.Join(absolute, "checkpoint.lmi"))
	missing := os.IsNotExist(err)
	if err != nil && !missing {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	if !missing {
		if _, err = os.Stat(filepath.Join(absolute, "journal.lmw")); err != nil {
			return nil, fmt.Errorf("checkpoint without journal; use validated restore")
		}
	}
	w, err := journal.Open(absolute, image.Sequence)
	if err != nil {
		return nil, err
	}
	// The blob store opens before replay and clears interrupted-write temp
	// files, so uncommitted or crashed writes never become permanent files.
	bs, err := blobs.Open(absolute)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("large-value store: %w", err)
	}
	d := &DB{dir: absolute, catalog: catalog.New(), store: versions.New(), blobStore: bs, wal: w, transactions: map[string]*Transaction{}}
	if !missing {
		d.catalog = image.Catalog
		d.store = image.Store
		d.sequence = image.Sequence
		d.horizon = image.Horizon
		// A checkpoint carries the full referenced blob closure; install any
		// blob that is not on disk yet (e.g. restored image) and verify it.
		if _, err = blobs.Install(absolute, blobs.Snapshot{Blobs: image.Blobs}); err != nil {
			w.Close()
			return nil, fmt.Errorf("checkpoint blobs: %w", err)
		}
	}
	last := w.Base + uint64(len(w.Records))
	if d.sequence > last {
		w.Close()
		return nil, fmt.Errorf("checkpoint ahead of journal")
	}
	for _, r := range w.Records {
		if r.Sequence <= d.sequence {
			continue
		}
		if err = d.apply(r); err != nil {
			w.Close()
			return nil, fmt.Errorf("replay sequence %d: %w", r.Sequence, err)
		}
	}
	// Rebuild reachability from every retained version and fail closed if a
	// referenced blob is missing; then orphan durable files are swept so the
	// bounded budget is not consumed by abandoned content.
	if err = d.blobStore.Reconcile(d.store.LargeRefs()); err != nil {
		w.Close()
		return nil, fmt.Errorf("large-value recovery: %w", err)
	}
	if _, _, err = d.blobStore.Sweep(d.store.LargeRefs()); err != nil {
		w.Close()
		return nil, fmt.Errorf("large-value recovery sweep: %w", err)
	}
	d.indexes = index.Build(d.catalog, d.store)
	for _, relation := range d.catalog.List() {
		if err = d.unique(relation.Name, nil); err != nil {
			w.Close()
			return nil, fmt.Errorf("recovered constraints: %w", err)
		}
	}
	return d, nil
}
func (d *DB) usable() error {
	if d.closed {
		return fault("unavailable", "database closed")
	}
	if d.wal.Poisoned {
		return fault("unavailable", "journal write outcome uncertain; restart required")
	}
	return nil
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.transactions = map[string]*Transaction{}
	return d.wal.Close()
}
func (d *DB) publish(r journal.Record) error {
	r.Sequence = d.sequence + 1
	r.Format = 1
	if r.Kind == "commit" {
		if err := d.externalize(r.Writes); err != nil {
			return err
		}
	}
	if err := d.wal.Append(r); err != nil {
		return fault("unavailable", "durable commit failed: %s", err)
	}
	if err := d.apply(r); err != nil {
		d.wal.Poisoned = true
		return fault("unavailable", "durable record cannot be applied: %s", err)
	}
	if len(r.Writes) > 0 {
		d.indexes.Apply(d.catalog, r.Writes)
	}
	return nil
}

// externalize moves every large_text payload out of the staged rows and into
// the content-addressed store before the journal frame is written. The row
// versions therefore persist only compact refs, so later small-column updates
// never rewrite the large value. Blob durability precedes WAL durability: if
// this fails nothing is appended and no sequence is consumed. Identical
// content resolves to one shared blob; small payloads stay inline in the row.
func (d *DB) externalize(writes []versions.Mutation) error {
	for i := range writes {
		if writes[i].Deleted {
			continue
		}
		relation, err := d.catalog.Relation(writes[i].Relation)
		if err != nil {
			return invalid(err)
		}
		row, err := relation.Normalize(writes[i].Row)
		if err != nil {
			return invalid(err)
		}
		for _, col := range relation.Columns {
			if col.Type != "large_text" {
				continue
			}
			v := row[col.Name]
			// References are minted only by the engine during externalization;
			// clients must always supply text (or null), never a forged ref.
			if v.Kind == "large_ref" {
				return fault("invalid", "column %s takes text; references are assigned internally", col.Name)
			}
			if v.Null() {
				continue
			}
			payload := []byte(v.Text)
			if len(payload) < blobs.InlineThreshold {
				continue
			}
			ref, err := d.blobStore.Write(payload)
			if err != nil {
				var budget *blobs.BudgetExceeded
				if errors.As(err, &budget) {
					// Clean admission rejection: no durable change and no
					// sequence consumed, so the instance keeps serving.
					return fault("capacity", "%s", err)
				}
				// A real write/fsync failure has an uncertain durable outcome;
				// stop serving and require restart verification, as for the
				// journal itself.
				d.wal.Poisoned = true
				return fault("unavailable", "large-value durability failed: %s", err)
			}
			row[col.Name] = datum.LargeValue(ref)
		}
		writes[i].Row = row
	}
	return nil
}
func (d *DB) Relations() ([]catalog.Relation, []catalog.Index, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, nil, err
	}
	return d.catalog.List(), d.catalog.IndexList(), nil
}
