// Package blobs owns the on-disk content-addressed large-value store.
//
// Rows never embed large payloads: they carry a datum.Ref containing the
// payload SHA-256 and length. This package stores the referenced bytes under
// data/blobs/<shard>/<hash>, deduplicating identical content, and is the only
// place that may delete blob files. Reclamation is reachability based: the
// engine hands it the set of refs still held by retained row versions, so a
// blob stays readable for as long as any visible (or snapshot-retained) row
// references it, and nothing else may be removed.
package blobs

import (
	"fmt"
	"io/fs"
	"lamina/internal/datum"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// MaxTotalBytes is the bounded extra space the instance reserves for large
// values. The design chooses precise reclamation and content sharing rather
// than raising this cap.
const MaxTotalBytes = 16 << 20

// InlineThreshold keeps small strings in the row instead of paying blob
// indirection for them; only payloads at least this large are externalized.
const InlineThreshold = 4096

type Store struct {
	dir     string
	refs    map[string]int // hash -> live reference count from in-memory versions
	bytes   map[string]int // hash -> payload size
	orphans map[string]int // durable files present on disk but unreferenced
}

// Snapshot is the portable, self-describing blob portion of an image. The map
// keys are content hashes and the values are raw payload bytes.
type Snapshot struct {
	Blobs map[string][]byte `json:"blobs"`
}

func Open(dir string) (*Store, error) {
	root := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	s := &Store{dir: root, refs: map[string]int{}, bytes: map[string]int{}, orphans: map[string]int{}}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func shardPath(root, hash string) string {
	return filepath.Join(root, hash[:2], hash)
}

// load enumerates durable files and removes interrupted-write leftovers
// (temporary files) so an aborted or crashed write can never become permanent
// occupancy. All complete files start out unreferenced; reachability is
// re-established by Reconcile once retained versions are known.
func (s *Store) load() error {
	tempFiles := []string{}
	err := filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".tmp-") {
			tempFiles = append(tempFiles, path)
			return nil
		}
		if len(name) != 64 {
			return fmt.Errorf("unexpected blob file %q", name)
		}
		if filepath.Base(filepath.Dir(path)) != name[:2] {
			return fmt.Errorf("blob %q stored in wrong shard", name)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		s.orphans[name] = int(info.Size())
		s.bytes[name] = int(info.Size())
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range tempFiles {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Hash returns the canonical ref hash for a payload.
func Hash(payload []byte) string {
	return datum.ContentHash(payload)
}

// BudgetExceeded reports that a write would push the bounded large-value
// footprint past MaxTotalBytes. It is a clean admission rejection rather than
// an uncertain storage failure.
type BudgetExceeded struct {
	Need int
}

func (e *BudgetExceeded) Error() string {
	return fmt.Sprintf("large-value budget %d bytes exceeded (need %d more)", MaxTotalBytes, e.Need)
}

// Write durably stores payload, returning its ref. Identical content is shared:
// an existing blob is never rewritten. The file is created in the shard
// directory as a temporary file, fsynced and atomically renamed, so a crash
// leaves only a removable temp file and never a partial durable blob.
func (s *Store) Write(payload []byte) (datum.Ref, error) {
	hash := Hash(payload)
	if _, ok := s.bytes[hash]; ok {
		return datum.Ref{Hash: hash, Length: len(payload)}, nil
	}
	overage := s.TotalBytes() + len(payload) - MaxTotalBytes
	if overage > 0 {
		return datum.Ref{}, &BudgetExceeded{Need: overage}
	}
	shard := filepath.Join(s.dir, hash[:2])
	if err := os.MkdirAll(shard, 0o700); err != nil {
		return datum.Ref{}, err
	}
	f, err := os.CreateTemp(shard, ".tmp-*")
	if err != nil {
		return datum.Ref{}, err
	}
	tmp := f.Name()
	cleanup := func() { _ = os.Remove(tmp) }
	if _, err = f.Write(payload); err != nil {
		f.Close()
		cleanup()
		return datum.Ref{}, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		cleanup()
		return datum.Ref{}, err
	}
	if err = f.Close(); err != nil {
		cleanup()
		return datum.Ref{}, err
	}
	final := shardPath(s.dir, hash)
	if err = os.Rename(tmp, final); err != nil {
		cleanup()
		return datum.Ref{}, err
	}
	if dir, err := os.Open(shard); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	s.bytes[hash] = len(payload)
	s.orphans[hash] = len(payload)
	return datum.Ref{Hash: hash, Length: len(payload)}, nil
}

// Read returns the payload for a stored ref, verifying length and digest so a
// corrupted or missing blob fails closed instead of producing a dangling row.
func (s *Store) Read(ref datum.Ref) ([]byte, error) {
	if !ref.Valid() {
		return nil, fmt.Errorf("invalid large-value reference")
	}
	payload, err := os.ReadFile(shardPath(s.dir, ref.Hash))
	if err != nil {
		return nil, fmt.Errorf("large value %s unreadable: %w", ref.Hash, err)
	}
	if len(payload) != ref.Length || Hash(payload) != ref.Hash {
		return nil, fmt.Errorf("large value %s fails digest verification", ref.Hash)
	}
	return payload, nil
}

func (s *Store) exists(ref datum.Ref) error {
	info, err := os.Stat(shardPath(s.dir, ref.Hash))
	if err != nil {
		return fmt.Errorf("large value %s missing: %w", ref.Hash, err)
	}
	if info.Size() != int64(ref.Length) {
		return fmt.Errorf("large value %s length mismatch", ref.Hash)
	}
	return nil
}

// Hold marks a ref as referenced by a retained in-memory version. The same
// content shared by many rows counts once per version that references it and
// is only deleted once every such version goes away.
func (s *Store) Hold(ref datum.Ref) {
	if !ref.Valid() {
		return
	}
	s.refs[ref.Hash]++
	delete(s.orphans, ref.Hash)
}

func (s *Store) reachable() map[string]bool {
	out := make(map[string]bool, len(s.refs))
	for hash := range s.refs {
		out[hash] = true
	}
	return out
}

// TotalBytes is the current on-disk footprint, including unreferenced files
// awaiting the next vacuum sweep.
func (s *Store) TotalBytes() int {
	total := 0
	for _, size := range s.bytes {
		total += size
	}
	return total
}

// Stats describes resource ownership for GET /stats.
type Stats struct {
	Blobs           int `json:"blobs"`
	ReferencedBlobs int `json:"referenced_blobs"`
	OrphanBlobs     int `json:"orphan_blobs"`
	TotalBytes      int `json:"total_bytes"`
	ReferencedBytes int `json:"referenced_bytes"`
	OrphanBytes     int `json:"orphan_bytes"`
	BudgetBytes     int `json:"budget_bytes"`
}

func (s *Store) Stats() Stats {
	out := Stats{Blobs: len(s.bytes), ReferencedBlobs: len(s.refs), OrphanBlobs: len(s.orphans), TotalBytes: s.TotalBytes(), BudgetBytes: MaxTotalBytes}
	for hash := range s.refs {
		out.ReferencedBytes += s.bytes[hash]
	}
	for _, size := range s.orphans {
		out.OrphanBytes += size
	}
	return out
}

// Reconcile rebuilds reference counts from the refs held by retained versions
// (used at startup and after image install) and verifies every referenced blob
// is present with the expected size; a missing blob is a hard failure rather
// than a silently dangling reference.
func (s *Store) Reconcile(refs []datum.Ref) error {
	s.refs = map[string]int{}
	s.orphans = map[string]int{}
	for hash, size := range s.bytes {
		s.orphans[hash] = size
	}
	for _, ref := range refs {
		if err := s.exists(ref); err != nil {
			return err
		}
		s.Hold(ref)
	}
	return nil
}

// Sweep removes durable blobs that no retained version references and returns
// the reaped blob count and byte total. Refs in the keep set are verified to
// exist first; any missing referenced blob aborts the sweep without deleting.
func (s *Store) Sweep(keep []datum.Ref) (reaped int, bytes int, err error) {
	wanted := map[string]bool{}
	for _, ref := range keep {
		if !ref.Valid() {
			return 0, 0, fmt.Errorf("invalid large-value reference in retained versions")
		}
		if err := s.exists(ref); err != nil {
			return 0, 0, err
		}
		wanted[ref.Hash] = true
	}
	hashes := make([]string, 0, len(s.bytes))
	for hash := range s.bytes {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		if wanted[hash] {
			continue
		}
		size := s.bytes[hash]
		if err := os.Remove(shardPath(s.dir, hash)); err != nil && !os.IsNotExist(err) {
			return reaped, bytes, err
		}
		delete(s.bytes, hash)
		delete(s.orphans, hash)
		delete(s.refs, hash)
		reaped++
		bytes += size
	}
	// Drop now-empty shard directories.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return reaped, bytes, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		shard := filepath.Join(s.dir, entry.Name())
		children, readErr := os.ReadDir(shard)
		if readErr != nil {
			return reaped, bytes, readErr
		}
		if len(children) == 0 {
			_ = os.Remove(shard)
		}
	}
	s.refs = map[string]int{}
	s.orphans = map[string]int{}
	for _, ref := range keep {
		s.Hold(ref)
	}
	return reaped, bytes, nil
}

// Capture freezes every reachable blob's payload for a checkpoint or backup.
// The closure is derived from retained versions by the caller; any missing
// referenced blob fails the export instead of producing an incomplete image.
func (s *Store) Capture(refs []datum.Ref) (Snapshot, error) {
	out := Snapshot{Blobs: map[string][]byte{}}
	seen := map[string]bool{}
	for _, ref := range refs {
		if seen[ref.Hash] {
			continue
		}
		seen[ref.Hash] = true
		payload, err := s.Read(ref)
		if err != nil {
			return Snapshot{}, err
		}
		out.Blobs[ref.Hash] = payload
	}
	return out, nil
}

// Install materializes an image's blob closure into a fresh data directory,
// validating each payload's digest and length and honoring the storage budget.
// It is used by restore and when a checkpoint supplies blobs missing locally.
func Install(dir string, snap Snapshot) (*Store, error) {
	s, err := Open(dir)
	if err != nil {
		return nil, err
	}
	hashes := make([]string, 0, len(snap.Blobs))
	for hash := range snap.Blobs {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		payload := snap.Blobs[hash]
		if Hash(payload) != hash {
			return nil, fmt.Errorf("image blob %s digest mismatch", hash)
		}
		if s.TotalBytes()+len(payload) > MaxTotalBytes {
			return nil, fmt.Errorf("image exceeds large-value budget")
		}
		if _, err := s.Write(payload); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// AllRefs enumerates refs currently counted as reachable in memory.
func (s *Store) AllRefs() []datum.Ref {
	out := make([]datum.Ref, 0, len(s.refs))
	for hash := range s.refs {
		out = append(out, datum.Ref{Hash: hash, Length: s.bytes[hash]})
	}
	return out
}
