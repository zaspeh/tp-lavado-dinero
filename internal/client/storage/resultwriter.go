package storage

import (
	"encoding/csv"
	"os"
	"path/filepath"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
)

type ResultCSVWriter struct {
	doneReceiving bool
	outputDir     string
	q1File        *os.File
	q1Writer      *csv.Writer
	q2File        *os.File
	q2Writer      *csv.Writer
}

func NewResultCSVWriter(outputDir string) *ResultCSVWriter {
	return &ResultCSVWriter{outputDir: outputDir, doneReceiving: false}
}

func (rw *ResultCSVWriter) Open() error {
	if err := os.MkdirAll(rw.outputDir, 0755); err != nil {
		return err
	}

	var err error
	rw.q1File, err = os.OpenFile(filepath.Join(rw.outputDir, "q1_result.csv"), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	rw.q1Writer = csv.NewWriter(rw.q1File)
	err = rw.q1Writer.Write([]string{"account", "to_account", "amount"})
	if err != nil {
		rw.q1File.Close()
		return err
	}

	rw.q2File, err = os.OpenFile(filepath.Join(rw.outputDir, "q2_result.csv"), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		rw.q1File.Close()
		return err
	}
	rw.q2Writer = csv.NewWriter(rw.q2File)
	err = rw.q2Writer.Write([]string{"bank_name", "account", "amount"})
	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		return err
	}

	return nil
}

func (rw *ResultCSVWriter) Close() {
	//TODO: MANEJAR ERROR DE CLOSE
	if rw.q1Writer != nil {
		rw.q1Writer.Flush()
	}
	if rw.q1File != nil {
		rw.q1File.Close()
	}

	if rw.q2Writer != nil {
		rw.q2Writer.Flush()
	}
	if rw.q2File != nil {
		rw.q2File.Close()
	}
}

func (rw *ResultCSVWriter) DoneReceiving() bool {
	return rw.doneReceiving
}

func (rw *ResultCSVWriter) HandleEOF(msg result.EOF) error {
	rw.doneReceiving = true
	return nil
}

func (rw *ResultCSVWriter) HandleMicrotransactionResult(msg result.MicrotransactionResult) error {
	for _, t := range msg.Transactions {

		record := []string{
			t.GetAccount(),
			t.GetToAccount(),
			t.GetAmountPaid(),
		}

		if err := rw.q1Writer.Write(record); err != nil {
			return err
		}
	}

	rw.q1Writer.Flush()

	return rw.q1Writer.Error()
}

func (rw *ResultCSVWriter) HandleMaxBankResult(msg result.MaxBankResult) error {
	record := []string{msg.BankName, msg.Account, msg.Amount}
	err := rw.q2Writer.Write(record)
	if err != nil {
		return err
	}

	rw.q2Writer.Flush()
	return rw.q2Writer.Error()
}
