package socket

import (
	"net"
)

type Socket struct {
	conn net.Conn
}

func New(conn net.Conn) *Socket {
	return &Socket{conn: conn}
}

func (s *Socket) WriteAll(data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		sended, err := s.conn.Write(data[totalSent:])
		if err != nil {
			return err
		}
		totalSent += sended
	}
	return nil
}

func (s *Socket) ReadAll(amountToRead int) ([]byte, error) {
	buffer := make([]byte, amountToRead)
	total_received := 0
	for total_received < amountToRead {
		received, err := s.conn.Read(buffer[total_received:])
		if err != nil {
			return nil, err
		}
		total_received += received
	}
	return buffer, nil
}

func (s *Socket) Close() error {
	return s.conn.Close()
}
