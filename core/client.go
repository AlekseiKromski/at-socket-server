package core

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
)

type Clients map[string]*Client

type Client struct {
	Conn *websocket.Conn
	ID   string
}

func CreateNewClient(clientID string, connection *websocket.Conn) *Client {
	return &Client{
		Conn: connection,
		ID:   clientID,
	}
}

func (c *Client) Handler(app *App) error {
	if err := c.Conn.WriteJSON(ActionModel{
		Action:  USER_ID,
		Payload: c.ID,
	}); err != nil {
		return fmt.Errorf("cannot send USER_ID: %v", err)
	}
	go c.startReceiveChannel(app)
	return nil
}

func (c *Client) Send(payload string, action HandlerName) error {
	am := ActionModel{
		Action:  action,
		Payload: payload,
	}

	return c.Conn.WriteJSON(am)
}

func (c *Client) startReceiveChannel(app *App) {
	defer func() {
		e := c.Conn.Close()
		if e != nil {
			return
		}
		app.removeClient(c.ID)
	}()

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if err, ok := err.(*websocket.CloseError); ok {
				app.sendHook(NewHook(CLIENT_CLOSED_CONNECTION, fmt.Sprintf("connection closed: %v", err)))
				return
			}

			app.sendHook(NewHook(ERROR, fmt.Sprintf("error in clinet: %v", err)))
			return
		}

		if app.Config.Debug {
			fmt.Printf("Payload: %s", string(message))
		}

		am := ActionModel{}
		if err := json.Unmarshal(message, &am); err != nil {
			if err := c.Conn.WriteJSON(ActionModel{
				Action:  ERR_DECODE,
				Payload: fmt.Sprintf("cannot decode your message: %v", err),
			}); err != nil {
				fmt.Printf("cannot send message: %v", err)
			}
		}

		handler := app.handlers.DefineHandler(am.Action)
		if handler == nil {
			app.sendHook(NewHook(ERROR, fmt.Sprintf("cannot define handler: %s", am.Action)))
			c.sendError(fmt.Sprintf("cannot define handler: %s", am.Action), app)
			continue
		}

		handler.Handle(am.Payload, c, app.Clients)
	}
}

func (c *Client) sendError(message string, app *App) {
	am := ActionModel{
		Action:  ERR_HANDLER,
		Payload: message,
	}

	if err := c.Conn.WriteJSON(am); err != nil {
		app.sendHook(NewHook(ERROR, fmt.Sprintf("cannot send message (%s): %v", message, err)))
	}
}
