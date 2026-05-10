package serializer

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	UINT32_SIZE  = 4
	UINT64_SIZE  = 8
	FLOAT64_SIZE = 8
	INT64_SIZE   = 8
)

func SerializeUint32(v uint32) []byte {
	bytes := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(bytes, v)
	return bytes
}

func DeserializeUint32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func SerializeString(s string) []byte {
	data := []byte(s)

	result := SerializeUint32(uint32(len(data)))
	result = append(result, data...)

	return result
}

func DeserializeString(data []byte) string {
	return string(data)
}

func SerializeBytes(data []byte) []byte {
	result := SerializeUint32(uint32(len(data)))
	result = append(result, data...)

	return result
}

func DeserializeBytes(data []byte) []byte {
	return data
}

func SerializeUint64(v uint64) []byte {
	bytes := make([]byte, UINT64_SIZE)
	binary.BigEndian.PutUint64(bytes, v)
	return bytes
}

func DeserializeUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func SerializeInt64(v int64) []byte {
	return SerializeUint64(uint64(v))
}

func DeserializeInt64(data []byte) int64 {
	return int64(DeserializeUint64(data))
}

func SerializeFloat64(v float64) []byte {
	return SerializeUint64(math.Float64bits(v))
}

func DeserializeFloat64(data []byte) float64 {
	return math.Float64frombits(
		DeserializeUint64(data),
	)
}

func ReadSizedField(data []byte, offset *int) ([]byte, error) {
	if *offset+UINT32_SIZE > len(data) {
		return nil, errors.New("invalid field size")
	}

	size := int(
		DeserializeUint32(data[*offset : *offset+UINT32_SIZE]),
	)

	*offset += UINT32_SIZE

	if *offset+size > len(data) {
		return nil, errors.New("invalid field payload")
	}

	field := data[*offset : *offset+size]
	*offset += size

	return field, nil
}
