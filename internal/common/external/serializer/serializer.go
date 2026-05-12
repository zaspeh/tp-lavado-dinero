package serializer

import (
	"encoding/binary"
)

const (
	Uint32Size = 4
	Uint16Size = 2
	ByteSize   = 1
)

func SerializeString(value string) []byte {
	return []byte(value)
}

func DeserializeString(bytes []byte) string {
	return string(bytes[:])
}

func SerializeUint8(value uint8) []byte {
	return []byte{value}
}

func DeserializeUint8(bytes []byte) uint8 {
	return bytes[0]
}

func SerializeUint16(value uint16) []byte {
	data := make([]byte, Uint16Size)
	binary.BigEndian.PutUint16(data, value)
	return data
}

func DeserializeUint16(bytes []byte) uint16 {
	return binary.BigEndian.Uint16(bytes)
}

func SerializeUint32(value uint32) []byte {
	data := make([]byte, Uint32Size)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func DeserializeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}
