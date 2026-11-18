package query

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"math"
	"sort"
)

func Execute(r catalog.Relation, q Request, rows map[string]datum.Row, plan Plan, keys []string) (Result, error) {
	result := Result{Rows: []datum.Row{}, Plan: plan}
	largeColumns := map[string]bool{}
	for _, c := range r.Columns {
		if c.Type == "large_text" {
			largeColumns[c.Name] = true
		}
	}
	if plan.Access == "scan" {
		keys = datum.Keys(rows)
	}
	selected := make([]datum.Row, 0)
	for _, key := range keys {
		row, ok := rows[key]
		if !ok {
			continue
		}
		result.Scanned++
		if MatchesTyped(row, q.Where, largeColumns) {
			selected = append(selected, row)
		}
	}
	result.Matched = len(selected)
	if q.Aggregate != nil {
		row, err := reduce(selected, *q.Aggregate)
		if err != nil {
			return result, err
		}
		result.Rows = append(result.Rows, row)
		return result, nil
	}
	sort.Slice(selected, func(i, j int) bool {
		for _, o := range q.Order {
			n := datum.Order(selected[i][o.Column], selected[j][o.Column])
			if n != 0 {
				if o.Desc {
					return n > 0
				}
				return n < 0
			}
		}
		return datum.Order(selected[i][r.Primary], selected[j][r.Primary]) < 0
	})
	limit := q.Limit
	if limit == 0 {
		limit = 1000
	}
	if len(selected) > limit {
		selected = selected[:limit]
		result.Truncated = true
	}
	for _, row := range selected {
		if len(q.Columns) == 0 {
			result.Rows = append(result.Rows, datum.Clone(row))
		} else {
			projection := datum.Row{}
			for _, name := range q.Columns {
				projection[name] = row[name]
			}
			result.Rows = append(result.Rows, projection)
		}
	}
	return result, nil
}
func reduce(rows []datum.Row, a Aggregate) (datum.Row, error) {
	var total int64
	var count int64
	for _, row := range rows {
		v := row[a.Column]
		if a.Column != "" && a.Column != "*" && v.Null() {
			continue
		}
		count++
		if a.Function == "sum" {
			if v.Int > 0 && total > math.MaxInt64-v.Int || v.Int < 0 && total < math.MinInt64-v.Int {
				return nil, fmt.Errorf("sum overflow")
			}
			total += v.Int
		}
	}
	if a.Function == "count" {
		return datum.Row{"count": datum.Integer(count)}, nil
	}
	if count == 0 {
		return datum.Row{"sum": datum.Value{Kind: "null"}}, nil
	}
	return datum.Row{"sum": datum.Integer(total)}, nil
}
