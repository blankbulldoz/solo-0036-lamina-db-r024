package catalog

import (
	"fmt"
	"lamina/internal/datum"
	"regexp"
)

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func ValidName(s string) bool { return identifier.MatchString(s) }

// MaxLargeText caps a single externalized value; it is intentionally smaller
// than the 1MiB request body so large values can enter through a single put.
const MaxLargeText = 512 << 10

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
type Relation struct {
	Name    string   `json:"name"`
	Primary string   `json:"primary"`
	Columns []Column `json:"columns"`
	Created uint64   `json:"created"`
}

func (r Relation) Validate() error {
	if !ValidName(r.Name) || !ValidName(r.Primary) {
		return fmt.Errorf("invalid relation or primary name")
	}
	if len(r.Columns) == 0 || len(r.Columns) > 64 {
		return fmt.Errorf("relation needs 1..64 columns")
	}
	seen := map[string]bool{}
	found := false
	for _, c := range r.Columns {
		if !ValidName(c.Name) || seen[c.Name] {
			return fmt.Errorf("invalid or duplicate column %q", c.Name)
		}
		seen[c.Name] = true
		if c.Type != "int" && c.Type != "text" && c.Type != "bool" && c.Type != "large_text" {
			return fmt.Errorf("unsupported type %q", c.Type)
		}
		if c.Name == r.Primary {
			found = true
			if c.Nullable || c.Type == "bool" || c.Type == "large_text" {
				return fmt.Errorf("primary must be non-null int or text")
			}
		}
	}
	if !found {
		return fmt.Errorf("primary column does not exist")
	}
	return nil
}
func (r Relation) Column(name string) (Column, bool) {
	for _, c := range r.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}
func (r Relation) Normalize(row datum.Row) (datum.Row, error) {
	for k := range row {
		if _, ok := r.Column(k); !ok {
			return nil, fmt.Errorf("unknown column %q", k)
		}
	}
	out := make(datum.Row, len(r.Columns))
	for _, c := range r.Columns {
		v, exists := row[c.Name]
		if !exists || v.Null() {
			if !c.Nullable {
				return nil, fmt.Errorf("column %s cannot be null", c.Name)
			}
			v = datum.Value{Kind: "null"}
		}
		if !v.Null() && v.Kind != c.Type && !(c.Type == "large_text" && v.Kind == "text") && !(c.Type == "large_text" && v.Kind == "large_ref") {
			return nil, fmt.Errorf("column %s requires %s", c.Name, c.Type)
		}
		if v.Kind == "text" && c.Type == "text" && len(v.Text) > 65536 {
			return nil, fmt.Errorf("text exceeds 64KiB")
		}
		if c.Type == "large_text" && v.Kind == "text" && len(v.Text) > MaxLargeText {
			return nil, fmt.Errorf("large_text exceeds %d bytes", MaxLargeText)
		}
		out[c.Name] = v
	}
	return out, nil
}
func (r Relation) Key(row datum.Row) string { return row[r.Primary].Key() }
