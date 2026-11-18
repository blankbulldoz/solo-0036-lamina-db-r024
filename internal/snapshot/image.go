package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"lamina/internal/catalog"
	"lamina/internal/versions"
	"os"
)

const MaxImage = 128 << 20

type Image struct {
	Format   int               `json:"format"`
	Sequence uint64            `json:"sequence"`
	Horizon  uint64            `json:"horizon"`
	Catalog  *catalog.Catalog  `json:"catalog"`
	Store    *versions.Store   `json:"store"`
	Blobs    map[string][]byte `json:"blobs"`
}
type Envelope struct {
	Format  int             `json:"format"`
	Digest  string          `json:"sha256"`
	Payload json.RawMessage `json:"payload"`
}

func Encode(image Image) ([]byte, error) {
	image.Format = 1
	b, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return json.Marshal(Envelope{Format: 1, Digest: hex.EncodeToString(h[:]), Payload: b})
}
func Decode(b []byte) (Image, error) {
	var image Image
	var env Envelope
	if len(b) > MaxImage {
		return image, fmt.Errorf("image exceeds limit")
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return image, err
	}
	h := sha256.Sum256(env.Payload)
	if env.Format != 1 || hex.EncodeToString(h[:]) != env.Digest {
		return image, fmt.Errorf("image format or digest mismatch")
	}
	if err := json.Unmarshal(env.Payload, &image); err != nil {
		return image, err
	}
	// Images written before large values omitted the blobs field; treat them
	// as an empty closure. Such images cannot contain large_ref rows, which
	// the reachability check below enforces.
	if image.Blobs == nil {
		image.Blobs = map[string][]byte{}
	}
	if err := Validate(image); err != nil {
		return image, err
	}
	return image, nil
}
func Read(path string) (Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return Image{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxImage+1))
	if err != nil {
		return Image{}, err
	}
	return Decode(b)
}
func Validate(image Image) error {
	if image.Format != 1 || image.Horizon > image.Sequence || image.Catalog == nil || image.Store == nil || image.Catalog.Relations == nil || image.Catalog.Indexes == nil || image.Store.Tables == nil || image.Blobs == nil {
		return fmt.Errorf("invalid image header")
	}
	for name, r := range image.Catalog.Relations {
		if name != r.Name || r.Created == 0 || r.Created > image.Sequence {
			return fmt.Errorf("invalid catalog sequence")
		}
		if err := r.Validate(); err != nil {
			return err
		}
	}
	for name, definition := range image.Catalog.Indexes {
		relation, ok := image.Catalog.Relations[definition.Relation]
		if !ok || name != definition.Name || !catalog.ValidName(name) || definition.Created < relation.Created || definition.Created > image.Sequence {
			return fmt.Errorf("invalid index metadata")
		}
		col, ok := relation.Column(definition.Column)
		if !ok {
			return fmt.Errorf("invalid index column")
		}
		if col.Type == "large_text" {
			return fmt.Errorf("large_text columns cannot be indexed")
		}
	}
	for name, rows := range image.Store.Tables {
		r, ok := image.Catalog.Relations[name]
		if !ok {
			return fmt.Errorf("image references unknown relation")
		}
		for key, chain := range rows {
			var last uint64
			if len(chain) == 0 {
				return fmt.Errorf("empty version chain")
			}
			for _, v := range chain {
				if v.Sequence <= last || v.Sequence > image.Sequence || v.Sequence <= r.Created {
					return fmt.Errorf("invalid version order")
				}
				last = v.Sequence
				if !v.Deleted {
					row, err := r.Normalize(v.Row)
					if err != nil || r.Key(row) != key {
						return fmt.Errorf("invalid stored tuple")
					}
				}
			}
		}
	}
	// Every externalized reference in a retained version must resolve to a
	// payload carried by the image, with matching length and digest. The image
	// is self-contained: a dangling or corrupted blob rejects the image before
	// any state is published by restore or checkpoint recovery.
	referenced := map[string]int{}
	for _, rows := range image.Store.Tables {
		for _, chain := range rows {
			for _, v := range chain {
				if v.Deleted {
					continue
				}
				for _, value := range v.Row {
					if ref, ok := value.LargeRef(); ok {
						referenced[ref.Hash] = ref.Length
					}
				}
			}
		}
	}
	for hash, length := range referenced {
		payload, ok := image.Blobs[hash]
		if !ok {
			return fmt.Errorf("image missing blob %s referenced by a retained version", hash)
		}
		if len(payload) != length {
			return fmt.Errorf("image blob %s length mismatch", hash)
		}
		h := sha256.Sum256(payload)
		if hex.EncodeToString(h[:]) != hash {
			return fmt.Errorf("image blob %s digest mismatch", hash)
		}
	}
	for hash, payload := range image.Blobs {
		h := sha256.Sum256(payload)
		if hex.EncodeToString(h[:]) != hash {
			return fmt.Errorf("image blob %s digest mismatch", hash)
		}
		if _, reachable := referenced[hash]; !reachable {
			return fmt.Errorf("image carries unreachable blob %s", hash)
		}
	}
	for _, definition := range image.Catalog.Indexes {
		if !definition.Unique {
			continue
		}
		seen := map[string]bool{}
		for _, chain := range image.Store.Tables[definition.Relation] {
			version, exists := versions.Visible(chain, image.Sequence)
			if !exists {
				continue
			}
			value := version.Row[definition.Column]
			if value.Null() {
				continue
			}
			if seen[value.Key()] {
				return fmt.Errorf("image violates unique index %s", definition.Name)
			}
			seen[value.Key()] = true
		}
	}

	return nil
}
