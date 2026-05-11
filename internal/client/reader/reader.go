package reader

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
)

const (
	dateLayout = "2006-01-02 15:04:05"
)

func ReadTransactions(
	path string,
	handler func(*protocol.Transaction) error,
) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return err
	}

	for {
		record, err := reader.Read()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		tx, err := parseTransaction(record)
		if err != nil {
			return err
		}

		if err := handler(tx); err != nil {
			return err
		}
	}
}

func parseTransaction(
	record []string,
) (*protocol.Transaction, error) {
	amount, err := strconv.ParseFloat(
		record[5],
		64,
	)
	if err != nil {
		return nil, err
	}

	timestamp, err := time.Parse(
		dateLayout,
		record[0],
	)
	if err != nil {
		return nil, err
	}

	return &protocol.Transaction{
		Timestamp:          timestamp.Unix(),
		BankOrigin:         record[1],
		AccountOrigin:      record[2],
		BankDestination:    record[3],
		AccountDestination: record[4],
		Amount:             amount,
		Currency:           record[6],
		PaymentType:        record[7],
	}, nil
}
