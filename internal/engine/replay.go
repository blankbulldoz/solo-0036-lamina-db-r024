package engine

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/journal"
)

func (d *DB) apply(r journal.Record) error {
	if r.Sequence != d.sequence+1 || r.Format != 1 {
		return fmt.Errorf("invalid record sequence")
	}
	switch r.Kind {
	case "relation":
		if r.Relation == nil || r.Relation.Created != r.Sequence {
			return fmt.Errorf("invalid relation record")
		}
		if err := r.Relation.Validate(); err != nil {
			return err
		}
		if _, ok := d.catalog.Relations[r.Relation.Name]; ok {
			return fmt.Errorf("duplicate relation")
		}
		d.catalog.Relations[r.Relation.Name] = *r.Relation
		d.store.Ensure(r.Relation.Name)
	case "index":
		if r.Index == nil || r.Index.Created != r.Sequence {
			return fmt.Errorf("invalid index record")
		}
		if err := d.catalog.CheckIndex(*r.Index); err != nil {
			return err
		}
		d.catalog.Indexes[r.Index.Name] = *r.Index
	case "commit":
		seen := map[string]bool{}
		for _, m := range r.Writes {
			relation, err := d.catalog.Relation(m.Relation)
			if err != nil {
				return err
			}
			token := m.Relation + "\x00" + m.Key
			if seen[token] {
				return fmt.Errorf("duplicate mutation")
			}
			seen[token] = true
			if !m.Deleted {
				row, err := relation.Normalize(m.Row)
				if err != nil {
					return err
				}
				if relation.Key(row) != m.Key {
					return fmt.Errorf("stored primary mismatch")
				}
			}
		}
		d.store.Apply(r.Sequence, r.Writes)
		for _, m := range r.Writes {
			if m.Deleted {
				continue
			}
			for _, value := range m.Row {
				if ref, ok := value.LargeRef(); ok {
					d.blobStore.Hold(ref)
				}
			}
		}
	case "vacuum":
		if r.Horizon < d.horizon || r.Horizon > d.sequence {
			return fmt.Errorf("invalid vacuum horizon")
		}
		d.store.Vacuum(r.Horizon)
		// Reclaim only blobs unreachable from the versions that survived
		// pruning. Active readers pin versions, which transitively pin their
		// blobs, so no visible row can be left dangling.
		if _, _, err := d.blobStore.Sweep(d.store.LargeRefs()); err != nil {
			return fmt.Errorf("large-value reclamation: %w", err)
		}
		d.horizon = r.Horizon
	default:
		return fmt.Errorf("unknown record kind %s", r.Kind)
	}
	d.sequence = r.Sequence
	return nil
}
func overlay(rows map[string]datum.Row, writes map[string]map[string]staged, table string) {
	for key, m := range writes[table] {
		if m.Mutation.Deleted {
			delete(rows, key)
		} else {
			rows[key] = datum.Clone(m.Mutation.Row)
		}
	}
}

// rejectClientRefs enforces that large_text values enter as text (or null).
// Content references are internal: only externalize may mint one, so a client
// cannot point a row at an arbitrary or nonexistent blob.
func rejectClientRefs(relation catalog.Relation, row datum.Row) error {
	for _, col := range relation.Columns {
		if col.Type == "large_text" && row[col.Name].Kind == "large_ref" {
			return fault("invalid", "column %s takes text; references are assigned internally", col.Name)
		}
	}
	return nil
}
