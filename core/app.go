package core

import (
	"fmt"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
)

type WebSocket struct{}

type HookType struct {
	HookType HookTypes
	Data     string
}

func NewHook(ht HookTypes, data string) HookType {
	return HookType{
		HookType: ht,
		Data:     data,
	}
}

type App struct {
	Hooks                  chan HookType
	Engine                 *gin.Engine
	Config                 *Config
	clients                Clients
	handlers               *Handlers
	server                 string
	httpConnectionUpgraded websocket.Upgrader
	mutex                  sync.Mutex
}

func Start(hs *Handlers, conf *Config) (*App, error) {
	app := App{Config: conf, clients: make(Clients), mutex: sync.Mutex{}}

	//Start application
	app.runApp(hs)

	//Configure server handlers and setup ws upgrader
	if err := app.serverConfigure(); err != nil {
		return nil, fmt.Errorf("cannot up server: %v", err)
	}

	return &app, nil
}

func (app *App) runApp(hs *Handlers) {
	app.initHooksChannel()
	app.registerHandlers(hs)
}

func (app *App) registerHandlers(hs *Handlers) {
	app.handlers = hs
}

func (app *App) initHooksChannel() {
	app.Hooks = make(chan HookType)
}

func (app *App) serverConfigure() error {
	ginEngine := gin.Default()

	app.httpConnectionUpgraded = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	ginEngine.GET("/ws/connect", func(c *gin.Context) {
		conn, err := app.httpConnectionUpgraded.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			fmt.Printf("problem while upgrade http connection to webscket: %v", err)
			return
		}
		client := app.addClient(conn)

		app.sendHook(NewHook(CLIENT_ADDED, client.ID))

		if err := client.Handler(app); err != nil {
			fmt.Printf("cannot handle client: %v", err)
		}
	})

	//Set cors
	ginEngine.Use(cors.New(app.Config.CorsOptions))

	app.Engine = ginEngine

	app.sendHook(NewHook(SERVER_STARTED, fmt.Sprintf("gin egine created")))
	return nil
}

func (app *App) sendHook(h HookType) {
	select {
	case app.Hooks <- h:
	default:
	}
}

func (app *App) addClient(conn *websocket.Conn) *Client {
	app.mutex.Lock()
	defer app.mutex.Unlock()

	for {
		c := CreateNewClient(conn)
		if app.clients[c.ID] == nil {
			app.clients[c.ID] = c
			return c
		}
	}
}

func (app *App) removeClient(id string) {
	app.mutex.Lock()
	defer app.mutex.Unlock()

	if app.clients[id] == nil {
		return
	}

	delete(app.clients, id)
}
