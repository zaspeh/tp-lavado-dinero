package storage

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
)

type ResultCSVWriter struct {
	doneReceiving bool
	outputDir     string
	q1File        *os.File
	q1Writer      *csv.Writer
	q2File        *os.File
	q2Writer      *csv.Writer
	q3File        *os.File
	q3Writer      *csv.Writer
	q4File        *os.File
	q4Writer      *csv.Writer
	q5File        *os.File
	q5Writer      *csv.Writer
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

	rw.q3File, err = os.OpenFile(filepath.Join(rw.outputDir, "q3_result.csv"), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		return err
	}

	rw.q3Writer = csv.NewWriter(rw.q3File)

	err = rw.q3Writer.Write([]string{"account", "amount"})
	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		rw.q3File.Close()
		return err
	}

	rw.q4File, err = os.OpenFile(filepath.Join(rw.outputDir, "q4_result.csv"), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		rw.q3File.Close()
		return err
	}

	rw.q4Writer = csv.NewWriter(rw.q4File)

	err = rw.q4Writer.Write([]string{"bank", "account"})

	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		rw.q3File.Close()
		rw.q4File.Close()
		return err
	}

	rw.q5File, err = os.OpenFile(filepath.Join(rw.outputDir, "q5_result.csv"), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		rw.q3File.Close()
		rw.q4File.Close()
		return err
	}
	rw.q5Writer = csv.NewWriter(rw.q5File)
	err = rw.q5Writer.Write([]string{"count"})
	if err != nil {
		rw.q1File.Close()
		rw.q2File.Close()
		rw.q3File.Close()
		rw.q4File.Close()
		rw.q5File.Close()
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
	if rw.q3Writer != nil {
		rw.q3Writer.Flush()
	}
	if rw.q3File != nil {
		rw.q3File.Close()
	}
	if rw.q4Writer != nil {
		rw.q4Writer.Flush()
	}
	if rw.q4File != nil {
		rw.q4File.Close()
	}
	if rw.q5Writer != nil {
		rw.q5Writer.Flush()
	}
	if rw.q5File != nil {
		rw.q5File.Close()
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
	parsedAmount := strconv.FormatFloat(msg.Amount, 'f', 2, 64)
	record := []string{msg.Account, msg.ToAccount, parsedAmount}
	err := rw.q1Writer.Write(record)
	if err != nil {
		return err
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

func (rw *ResultCSVWriter) HandleConvertedMicroPaymentResult(msg result.ConvertedMicroPaymentResult) error {
	countStr := strconv.FormatInt(int64(msg.Count), 10)
	err := rw.q5Writer.Write([]string{countStr})
	if err != nil {
		return err
	}
	rw.q5Writer.Flush()
	return rw.q5Writer.Error()
}

func (rw *ResultCSVWriter) HandleAvgByTypeResult(msg result.AvgByTypeResult) error {

	record := []string{
		msg.Account,
		msg.AmountPaid,
	}

	if err := rw.q3Writer.Write(record); err != nil {
		return err
	}

	rw.q3Writer.Flush()

	return rw.q3Writer.Error()
}

func (rw *ResultCSVWriter) HandleSuspiciousAccountsResult(msg result.SuspiciousAccountsResult) error {
	for _, account := range msg.Accounts {

		record := []string{
			strconv.Itoa(int(account.Bank)),
			account.Account,
		}

		if err := rw.q4Writer.Write(record); err != nil {
			return err
		}
	}

	rw.q4Writer.Flush()

	return rw.q4Writer.Error()
}
