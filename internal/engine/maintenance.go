package engine

import (
	"fmt"
	"lamina/internal/blobs"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
)

func (d *DB) image() (snapshot.Image, error) {
	snap, err := d.blobStore.Capture(d.store.LargeRefs())
	if err != nil {
		return snapshot.Image{}, fault("unavailable", "large-value capture: %s", err)
	}
	return snapshot.Image{Format: 1, Sequence: d.sequence, Horizon: d.horizon, Catalog: d.catalog.Clone(), Store: d.store.Clone(), Blobs: snap.Blobs}, nil
}
func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	image, err := d.image()
	if err != nil {
		return 0, err
	}
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), image); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	return d.sequence, nil
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	image, err := d.image()
	if err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(image)
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}
func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.RemoveAll(filepath.Join(dir, "blobs"))
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	// Materialize and verify the blob closure before the journal is created so
	// a failed restore leaves no partially usable data directory.
	if _, err = blobs.Install(dir, blobs.Snapshot{Blobs: image.Blobs}); err != nil {
		return fmt.Errorf("restore blobs: %w", err)
	}
	w, err := journal.Open(dir, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}
func (d *DB) Vacuum(requested uint64) (versions.VacuumResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return versions.VacuumResult{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	horizon := requested
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	beforeKeys, beforeVersions := d.store.Count()
	_ = beforeKeys
	beforeBlobs := d.blobStore.Stats()
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		return versions.VacuumResult{}, err
	}
	// publish applied the vacuum record, which already swept unreachable
	// blobs; report the reclamation and the post-vacuum resource ownership.
	_, remaining := d.store.Count()
	d.indexes = index.Build(d.catalog, d.store)
	blobStats := d.blobStore.Stats()
	return versions.VacuumResult{
		Horizon:     horizon,
		Removed:     beforeVersions - remaining,
		Retained:    remaining,
		Pinned:      horizon < requested,
		BlobsReaped: beforeBlobs.Blobs - blobStats.Blobs,
		BlobBytes:   blobStats.ReferencedBytes,
		BlobBudget:  blobStats.BudgetBytes,
	}, nil
}
