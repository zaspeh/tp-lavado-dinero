package serializer

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
)

func SerializeEOF(eof *protocol.EOF) ([]byte, error) {
	data := SerializeString(eof.JobID)

	data = append(data,
		SerializeString(eof.Source)...,
	)

	data = append(data,
		SerializeString(eof.QueryID)...,
	)

	return data, nil
}

func DeserializeEOF(data []byte) (*protocol.EOF, error) {
	offset := 0

	jobIDBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	sourceBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	queryIDBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	return &protocol.EOF{
		JobID:   DeserializeString(jobIDBytes),
		Source:  DeserializeString(sourceBytes),
		QueryID: DeserializeString(queryIDBytes),
	}, nil
}
