package filterprocessor

import (
	"slices"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type FormatFilterProcessor struct {
	allowedFormats []string
}

func NewFormatFilterProcessor(allowedFormats []string) *FormatFilterProcessor {
	return &FormatFilterProcessor{
		allowedFormats: allowedFormats,
	}
}

func (f *FormatFilterProcessor) Process(clientID string, msg *protobuf.ToConvertPeriodFiltered) ([]*protobuf.ToConvertTypeFilteredPayment, bool, error) {
	paymentFormat := msg.GetPaymentFormat()

	if !f.isAllowedFormat(paymentFormat) {
		return nil, false, nil
	}

	filteredMsg := &protobuf.ToConvertTypeFilteredPayment{
		AmountPaid:      msg.GetAmountPaid(),
		PaymentCurrency: msg.GetPaymentCurrency(),
		Timestamp:       msg.GetTimestamp(),
	}
	return []*protobuf.ToConvertTypeFilteredPayment{filteredMsg}, false, nil
}

func (f *FormatFilterProcessor) isAllowedFormat(format string) bool {
	return slices.Contains(f.allowedFormats, format)
}
