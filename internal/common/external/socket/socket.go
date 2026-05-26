package socket

import (
	"errors"
	"io"
	"net"
)

var ErrConnectionClosed = errors.New("socket: connection closed by peer")

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
			if isDisconnection(err) {
				return ErrConnectionClosed
			}
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
			if isDisconnection(err) {
				return nil, ErrConnectionClosed
			}
			return nil, err
		}
		total_received += received
	}
	return buffer, nil
}

func (s *Socket) Close() error {
	return s.conn.Close()
}

func isDisconnection(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return !netErr.Temporary()
	}
	return false
}
