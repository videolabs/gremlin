package gremlin

import (
	"net/http"

	"github.com/gorilla/websocket"
)

type Conn struct {
	ws *websocket.Conn
}

func NewConn() (*Conn, error) {
	// Open a TCP connection
	conn, server, err := CreateConnection()
	if err != nil {
		return nil, err
	}
	// Open a new socket connection
	ws, _, err := websocket.NewClient(conn, server, http.Header{}, 0, 8192)
	if err != nil {
		return nil, err
	}
	return &Conn{ws: ws}, nil
}

func (c *Conn) Close() error {
	return c.ws.Close()
}
