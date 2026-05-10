package serializer

import (
	"tp-lavado-dinero/common/external/protocol"
)

func SerializeResult(result *protocol.Result) ([]byte, error) {
	data := SerializeString(result.JobID)

	data = append(data,
		SerializeString(result.Query)...,
	)

	data = append(data,
		SerializeBytes(result.Data)...,
	)

	return data, nil
}

func DeserializeResult(data []byte) (*protocol.Result, error) {
	offset := 0

	jobIDBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	queryBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	dataBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	return &protocol.Result{
		JobID: DeserializeString(jobIDBytes),
		Query: DeserializeString(queryBytes),
		Data:  DeserializeBytes(dataBytes),
	}, nil
}
