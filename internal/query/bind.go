package query

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
)

func Bind(r catalog.Relation, q Request) error {
	if q.Limit < 0 || q.Limit > 10000 {
		return fmt.Errorf("limit must be 0..10000")
	}
	if len(q.Where) > 32 || len(q.Order) > 8 {
		return fmt.Errorf("query expression limit exceeded")
	}
	if q.Aggregate != nil && len(q.Columns) > 0 {
		return fmt.Errorf("projection and aggregate cannot be combined")
	}
	for _, name := range q.Columns {
		if _, ok := r.Column(name); !ok {
			return fmt.Errorf("unknown projection %s", name)
		}
	}
	for _, o := range q.Order {
		c, ok := r.Column(o.Column)
		if !ok {
			return fmt.Errorf("unknown ordering %s", o.Column)
		}
		if c.Type == "large_text" {
			return fmt.Errorf("large_text column %s cannot be ordered", o.Column)
		}
	}
	for _, p := range q.Where {
		c, ok := r.Column(p.Column)
		if !ok {
			return fmt.Errorf("unknown predicate %s", p.Column)
		}
		switch p.Op {
		case "=", "!=", "<", ">", "<=", ">=", "is_null", "not_null":
		default:
			return fmt.Errorf("unsupported comparison %s", p.Op)
		}
		if c.Type == "large_text" {
			switch p.Op {
			case "=", "!=", "is_null", "not_null":
			default:
				return fmt.Errorf("large_text only supports equality and null predicates")
			}
		}
		if p.Op != "is_null" && p.Op != "not_null" && !p.Value.Null() && p.Value.Kind != c.Type && !(c.Type == "large_text" && p.Value.Kind == "text") {
			return fmt.Errorf("predicate type mismatch for %s", p.Column)
		}
	}
	if a := q.Aggregate; a != nil {
		if a.Function != "count" && a.Function != "sum" {
			return fmt.Errorf("unsupported aggregate")
		}
		if a.Column == "*" || a.Column == "" {
			if a.Function != "count" {
				return fmt.Errorf("sum needs integer column")
			}
		} else {
			c, ok := r.Column(a.Column)
			if !ok {
				return fmt.Errorf("unknown aggregate column")
			}
			if a.Function == "sum" && c.Type != "int" {
				return fmt.Errorf("sum requires integer column")
			}
		}
	}
	return nil
}
func Matches(row datum.Row, predicates []Predicate) bool {
	return MatchesTyped(row, predicates, nil)
}

// MatchesTyped behaves like Matches but uses content-based equality for
// large_text columns: an externalized reference and the inline text carrying
// the same bytes must compare equal. Columns named in largeColumns are treated
// as large_text regardless of value representation.
func MatchesTyped(row datum.Row, predicates []Predicate, largeColumns map[string]bool) bool {
	for _, p := range predicates {
		v := row[p.Column]
		if p.Op == "is_null" {
			if !v.Null() {
				return false
			}
			continue
		}
		if p.Op == "not_null" {
			if v.Null() {
				return false
			}
			continue
		}
		if v.Null() || p.Value.Null() {
			return false
		}
		if largeColumns != nil && largeColumns[p.Column] {
			if p.Op != "=" && p.Op != "!=" {
				return false
			}
			equal, err := datum.LargeEqual(v, p.Value)
			if err != nil {
				return false
			}
			if (p.Op == "=") != equal {
				return false
			}
			continue
		}
		n, err := v.Compare(p.Value)
		if err != nil {
			return false
		}
		pass := false
		switch p.Op {
		case "=":
			pass = n == 0
		case "!=":
			pass = n != 0
		case "<":
			pass = n < 0
		case ">":
			pass = n > 0
		case "<=":
			pass = n <= 0
		case ">=":
			pass = n >= 0
		}
		if !pass {
			return false
		}
	}
	return true
}
