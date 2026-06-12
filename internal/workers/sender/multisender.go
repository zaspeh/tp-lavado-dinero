package sender

type MultiSender[V any] struct {
	senders []Sender[V]
}

func NewMultiSender[V any](senders ...Sender[V]) *MultiSender[V] {
	return &MultiSender[V]{
		senders: senders,
	}
}

func (s *MultiSender[V]) Add(clientID string, item V, batchID string) error {
	for _, sender := range s.senders {
		if err := sender.Add(clientID, item, batchID); err != nil {
			return err
		}
	}
	return nil
}

func (s *MultiSender[V]) Flush(clientID string) error {
	for _, sender := range s.senders {
		if err := sender.Flush(clientID); err != nil {
			return err
		}
	}
	return nil
}

func (s *MultiSender[V]) Cleanup(clientID string) error {
	for _, sender := range s.senders {
		if err := sender.Cleanup(clientID); err != nil {
			return err
		}
	}
	return nil
}

func (s *MultiSender[V]) SendEOF(clientID string, survivorCount uint64) error {
	for _, sender := range s.senders {
		if err := sender.SendEOF(clientID, survivorCount); err != nil {
			return err
		}
	}
	return nil
}

func (s *MultiSender[V]) Close() error {
	for _, sender := range s.senders {
		if err := sender.Close(); err != nil {
			return err
		}
	}
	return nil
}
