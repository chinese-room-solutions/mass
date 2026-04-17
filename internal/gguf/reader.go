// Package gguf reads GGUF file header metadata without loading tensors.
//
// GGUF v3 (little-endian): magic "GGUF", uint32 version, uint64 tensor
// count, uint64 KV count, then KV pairs (key string + uint32 type + value).
package gguf

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/KernelPryanic/ctxerr"
)

// Value type constants from the GGUF spec.
const (
	typeUint8   uint32 = 0
	typeInt8    uint32 = 1
	typeUint16  uint32 = 2
	typeInt16   uint32 = 3
	typeUint32  uint32 = 4
	typeInt32   uint32 = 5
	typeFloat32 uint32 = 6
	typeBool    uint32 = 7
	typeString  uint32 = 8
	typeArray   uint32 = 9
	typeUint64  uint32 = 10
	typeInt64   uint32 = 11
	typeFloat64 uint32 = 12
)

// Meta holds the parsed GGUF header metadata.
type Meta struct {
	Version     uint32
	TensorCount uint64
	KV          map[string]any // values: string, uint32, int32, float32, uint64, int64, float64, bool, []any
}

// GetString returns a string value, or "" if not found or wrong type.
func (m *Meta) GetString(key string) string {
	if v, ok := m.KV[key].(string); ok {
		return v
	}
	return ""
}

// GetUint32 returns a uint32 value, or 0 if not found or wrong type.
func (m *Meta) GetUint32(key string) uint32 {
	if v, ok := m.KV[key].(uint32); ok {
		return v
	}
	return 0
}

// GetUint64 returns a uint64 value, or 0. Also handles uint32 values.
func (m *Meta) GetUint64(key string) uint64 {
	switch v := m.KV[key].(type) {
	case uint64:
		return v
	case uint32:
		return uint64(v)
	}
	return 0
}

// GetInt32 returns an int32 value, or 0 if not found.
func (m *Meta) GetInt32(key string) int32 {
	if v, ok := m.KV[key].(int32); ok {
		return v
	}
	return 0
}

// GetFloat32 returns a float32 value, or 0 if not found.
func (m *Meta) GetFloat32(key string) float32 {
	if v, ok := m.KV[key].(float32); ok {
		return v
	}
	return 0
}

// GetArrayLen returns the length of an array value, or 0.
func (m *Meta) GetArrayLen(key string) int {
	if v, ok := m.KV[key].([]any); ok {
		return len(v)
	}
	return 0
}

// ReadMeta opens a GGUF file and reads only the header metadata.
// No tensor data is loaded.
func ReadMeta(path string) (*Meta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("opening GGUF file: %w", err), map[string]any{"path": path})
	}
	defer f.Close() //nolint:errcheck // read-only file

	r := &reader{r: f}

	// Magic: "GGUF" (4 bytes).
	var magic [4]byte
	if err := binary.Read(f, binary.LittleEndian, &magic); err != nil {
		return nil, fmt.Errorf("reading magic: %w", err)
	}
	if magic != [4]byte{'G', 'G', 'U', 'F'} {
		return nil, ctxerr.With(fmt.Errorf("not a GGUF file (magic: %q)", magic), map[string]any{"path": path})
	}

	// Version.
	version, err := r.u32()
	if err != nil {
		return nil, fmt.Errorf("reading version: %w", err)
	}
	if version < 2 || version > 3 {
		return nil, ctxerr.With(fmt.Errorf("unsupported GGUF version %d", version), map[string]any{"path": path})
	}

	// Tensor count.
	tensorCount, err := r.u64()
	if err != nil {
		return nil, fmt.Errorf("reading tensor count: %w", err)
	}

	// Metadata KV count.
	kvCount, err := r.u64()
	if err != nil {
		return nil, fmt.Errorf("reading KV count: %w", err)
	}

	// Safety: refuse absurd KV counts (corrupt file).
	if kvCount > 100_000 {
		return nil, ctxerr.With(fmt.Errorf("KV count too large (%d), file may be corrupt", kvCount), map[string]any{"path": path})
	}

	meta := &Meta{
		Version:     version,
		TensorCount: tensorCount,
		KV:          make(map[string]any, kvCount),
	}

	// Read KV pairs.
	for i := uint64(0); i < kvCount; i++ {
		key, err := r.str()
		if err != nil {
			return nil, fmt.Errorf("reading key %d: %w", i, err)
		}
		val, err := r.value()
		if err != nil {
			return nil, fmt.Errorf("reading value for key %q: %w", key, err)
		}
		meta.KV[key] = val
	}

	return meta, nil
}

// reader wraps an io.Reader for little-endian binary reading.
type reader struct {
	r io.Reader
}

func (r *reader) u8() (uint8, error) {
	var v uint8
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) i8() (int8, error) {
	var v int8
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) u16() (uint16, error) {
	var v uint16
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) i16() (int16, error) {
	var v int16
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) u32() (uint32, error) {
	var v uint32
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) i32() (int32, error) {
	var v int32
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) u64() (uint64, error) {
	var v uint64
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) i64() (int64, error) {
	var v int64
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) f32() (float32, error) {
	var v float32
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) f64() (float64, error) {
	var v float64
	return v, binary.Read(r.r, binary.LittleEndian, &v)
}

func (r *reader) boolean() (bool, error) {
	b, err := r.u8()
	return b != 0, err
}

func (r *reader) str() (string, error) {
	n, err := r.u64()
	if err != nil {
		return "", err
	}
	if n > 1<<20 { // 1 MB safety limit for a single string
		return "", fmt.Errorf("string too long (%d bytes)", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// value reads a typed value (type tag + payload).
func (r *reader) value() (any, error) {
	vtype, err := r.u32()
	if err != nil {
		return nil, err
	}
	return r.typedValue(vtype)
}

// typedValue reads a value of the given type.
func (r *reader) typedValue(vtype uint32) (any, error) {
	switch vtype {
	case typeUint8:
		v, err := r.u8()
		return uint32(v), err // promote to uint32 for simplicity
	case typeInt8:
		v, err := r.i8()
		return int32(v), err // promote to int32
	case typeUint16:
		v, err := r.u16()
		return uint32(v), err // promote
	case typeInt16:
		v, err := r.i16()
		return int32(v), err // promote
	case typeUint32:
		return r.u32()
	case typeInt32:
		return r.i32()
	case typeFloat32:
		return r.f32()
	case typeBool:
		return r.boolean()
	case typeString:
		return r.str()
	case typeUint64:
		return r.u64()
	case typeInt64:
		return r.i64()
	case typeFloat64:
		return r.f64()
	case typeArray:
		return r.array()
	default:
		return nil, fmt.Errorf("unknown GGUF value type %d", vtype)
	}
}

func (r *reader) array() ([]any, error) {
	elemType, err := r.u32()
	if err != nil {
		return nil, err
	}
	n, err := r.u64()
	if err != nil {
		return nil, err
	}
	if n > 10_000_000 { // safety limit
		return nil, fmt.Errorf("array too large (%d elements)", n)
	}
	arr := make([]any, n)
	for i := uint64(0); i < n; i++ {
		arr[i], err = r.typedValue(elemType)
		if err != nil {
			return nil, fmt.Errorf("reading array element %d: %w", i, err)
		}
	}
	return arr, nil
}
