package socket

import (
	"io"

	"tp-lavado-dinero/common/external/serializer"
)

func Write(writer io.Writer, data []byte) error {
	size := serializer.SerializeUint32(uint32(len(data)))

	if err := WriteAll(writer, size); err != nil {
		return err
	}

	return WriteAll(writer, data)
}

func Read(reader io.Reader) ([]byte, error) {
	sizeBytes, err := ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}

	size := serializer.DeserializeUint32(sizeBytes)

	data, err := ReadAll(reader, int(size))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func ReadAll(reader io.Reader, size int) ([]byte, error) {
	data := make([]byte, size)

	total := 0

	for total < size {
		n, err := reader.Read(data[total:])
		if err != nil {
			return nil, err
		}

		total += n
	}

	return data, nil
}

func WriteAll(writer io.Writer, data []byte) error {
	total := 0

	for total < len(data) {
		n, err := writer.Write(data[total:])
		if err != nil {
			return err
		}

		total += n
	}

	return nil
}
