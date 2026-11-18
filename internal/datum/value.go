package datum

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
)

// Ref is the compact, content-addressed pointer persisted inside a row in
// place of a large value. Hash is the lowercase hex SHA-256 of the payload.
type Ref struct {
	Hash   string `json:"hash"`
	Length int    `json:"length"`
}

func (r Ref) Valid() bool {
	if len(r.Hash) != 64 || r.Length < 0 {
		return false
	}
	_, err := hex.DecodeString(r.Hash)
	return err == nil
}

type Value struct {
	Kind string
	Int  int64
	Text string
	Bool bool
	Ref  Ref
}

func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Kind {
	case "", "null":
		return []byte("null"), nil
	case "int":
		return []byte(strconv.FormatInt(v.Int, 10)), nil
	case "text":
		return json.Marshal(v.Text)
	case "bool":
		return json.Marshal(v.Bool)
	case "large_ref":
		return json.Marshal(struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Bytes int    `json:"bytes"`
		}{Type: "large_ref", Hash: v.Ref.Hash, Bytes: v.Ref.Length})
	default:
		return nil, fmt.Errorf("unknown value kind %q", v.Kind)
	}
}
func (v *Value) UnmarshalJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var raw any
	if err := d.Decode(&raw); err != nil {
		return err
	}
	*v = Value{Kind: "null"}
	switch x := raw.(type) {
	case nil:
	case string:
		v.Kind = "text"
		v.Text = x
	case bool:
		v.Kind = "bool"
		v.Bool = x
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return fmt.Errorf("only signed 64-bit integers are supported")
		}
		v.Kind = "int"
		v.Int = n
	case map[string]any:
		typ, _ := x["type"].(string)
		if typ != "large_ref" {
			return fmt.Errorf("scalar value required")
		}
		hash, _ := x["hash"].(string)
		length, hasLength := x["bytes"].(json.Number)
		if !hasLength {
			return fmt.Errorf("large_ref requires bytes")
		}
		n, err := length.Int64()
		if err != nil || n < 0 {
			return fmt.Errorf("large_ref requires a byte length")
		}
		ref := Ref{Hash: hash, Length: int(n)}
		if !ref.Valid() {
			return fmt.Errorf("invalid large_ref")
		}
		v.Kind = "large_ref"
		v.Ref = ref
	default:
		return fmt.Errorf("scalar value required")
	}
	return nil
}
func (v Value) Null() bool { return v.Kind == "" || v.Kind == "null" }
func (v Value) LargeRef() (Ref, bool) {
	if v.Kind == "large_ref" {
		return v.Ref, true
	}
	return Ref{}, false
}
func (v Value) Compare(w Value) (int, error) {
	if v.Null() || w.Null() {
		return 0, fmt.Errorf("null has no predicate ordering")
	}
	if v.Kind != w.Kind {
		return 0, fmt.Errorf("incompatible types %s and %s", v.Kind, w.Kind)
	}
	switch v.Kind {
	case "int":
		if v.Int < w.Int {
			return -1, nil
		}
		if v.Int > w.Int {
			return 1, nil
		}
	case "text":
		if v.Text < w.Text {
			return -1, nil
		}
		if v.Text > w.Text {
			return 1, nil
		}
	case "large_ref":
		if v.Ref.Hash < w.Ref.Hash {
			return -1, nil
		}
		if v.Ref.Hash > w.Ref.Hash {
			return 1, nil
		}
	case "bool":
		if !v.Bool && w.Bool {
			return -1, nil
		}
		if v.Bool && !w.Bool {
			return 1, nil
		}
	}
	return 0, nil
}
func (v Value) Key() string {
	b, _ := v.MarshalJSON()
	return v.Kind + ":" + string(b)
}
func Integer(n int64) Value { return Value{Kind: "int", Int: n} }
func LargeValue(r Ref) Value {
	return Value{Kind: "large_ref", Ref: r}
}

// ContentHash is the canonical content digest used both by the blob store and
// by predicate evaluation, so an inline payload and its externalized reference
// compare as the same content.
func ContentHash(payload []byte) string {
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

// LargeEqual compares two representations of a large_text column: a row may
// carry inline text (a staged write or a small value) or a content reference,
// while query predicates always arrive as text. Equality is content based, so
// the same bytes match regardless of whether they have been externalized.
func LargeEqual(a, b Value) (bool, error) {
	if a.Null() || b.Null() {
		return false, fmt.Errorf("null large value has no equality")
	}
	switch {
	case a.Kind == "text" && b.Kind == "text":
		return a.Text == b.Text, nil
	case a.Kind == "large_ref" && b.Kind == "large_ref":
		return a.Ref.Hash == b.Ref.Hash && a.Ref.Length == b.Ref.Length, nil
	case a.Kind == "large_ref" && b.Kind == "text":
		return a.Ref.Length == len(b.Text) && a.Ref.Hash == ContentHash([]byte(b.Text)), nil
	case a.Kind == "text" && b.Kind == "large_ref":
		return b.Ref.Length == len(a.Text) && b.Ref.Hash == ContentHash([]byte(a.Text)), nil
	default:
		return false, fmt.Errorf("incompatible large-value representations %s and %s", a.Kind, b.Kind)
	}
}
