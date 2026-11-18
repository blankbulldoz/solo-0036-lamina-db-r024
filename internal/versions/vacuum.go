package versions

type VacuumResult struct {
	Horizon     uint64 `json:"horizon"`
	Removed     int    `json:"removed"`
	Retained    int    `json:"retained"`
	Pinned      bool   `json:"pinned"`
	BlobsReaped int    `json:"blobs_reaped"`
	BlobBytes   int    `json:"blob_bytes_retained"`
	BlobBudget  int    `json:"blob_budget_bytes"`
}

func (s *Store) Vacuum(horizon uint64) VacuumResult {
	result := VacuumResult{Horizon: horizon}
	for _, rows := range s.Tables {
		for key, chain := range rows {
			keep := 0
			for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
				keep++
			}
			if len(chain) > 0 && chain[len(chain)-1].Sequence <= horizon && chain[len(chain)-1].Deleted {
				result.Removed += len(chain)
				delete(rows, key)
				continue
			}
			result.Removed += keep
			remaining := append([]Version(nil), chain[keep:]...)
			rows[key] = remaining
			result.Retained += len(remaining)
		}
	}
	return result
}
