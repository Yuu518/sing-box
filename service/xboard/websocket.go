package xboard

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"

	"github.com/coder/websocket"
)

const (
	webSocketRetryInterval  = 10 * time.Second
	webSocketStatusInterval = 10 * time.Second
)

type webSocketSession struct {
	client    *http.Client
	url       string
	query     url.Values
	connected *atomic.Bool
	name      string
	handle    func(payload []byte, pong func() error) error
	status    func() []map[string]any
}

func (s *Service) loopWebSocket(managedNode *node) {
	ctx := s.nodeContext(managedNode)
	s.loopWebSocketSession(ctx, managedNode.logFields(), func() error {
		return s.runWebSocket(ctx, managedNode)
	})
}

func (s *Service) loopWebSocketSession(ctx context.Context, name string, run func() error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := run()
		if errors.Is(err, websocket.ErrMessageTooBig) || websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
			s.logger.Warn("websocket message exceeds read limit, using polling: ", name)
			return
		}
		if errors.Is(err, errWebSocketDisabled) {
			s.logger.Info("websocket disabled, using polling: ", name)
			return
		}
		if err != nil && !E.IsClosedOrCanceled(err) {
			s.logger.Warn("websocket disconnected: ", name, ": ", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(webSocketRetryInterval):
		}
	}
}

func (s *Service) runWebSocket(ctx context.Context, managedNode *node) error {
	wsURL, err := s.handshake(ctx, managedNode)
	if err != nil {
		return err
	}
	if wsURL == "" {
		return errWebSocketDisabled
	}
	query := make(url.Values)
	query.Set("token", managedNode.panel.options.Token)
	query.Set("node_id", F.ToString(managedNode.options.NodeID))
	return s.serveWebSocket(ctx, webSocketSession{
		client:    managedNode.panel.client,
		url:       wsURL,
		query:     query,
		connected: &managedNode.wsConnected,
		name:      managedNode.logFields(),
		handle: func(payload []byte, pong func() error) error {
			return s.handleWebSocketMessage(ctx, managedNode, payload, pong)
		},
		status: func() []map[string]any {
			return []map[string]any{s.buildWebSocketStatus(managedNode)}
		},
	})
}

func (s *Service) serveWebSocket(ctx context.Context, session webSocketSession) error {
	parsedURL, err := url.Parse(session.url)
	if err != nil {
		return err
	}
	query := parsedURL.Query()
	query.Set("sync_mode", "notify")
	for key, values := range session.query {
		query[key] = values
	}
	parsedURL.RawQuery = query.Encode()
	conn, _, err := websocket.Dial(ctx, parsedURL.String(), &websocket.DialOptions{
		HTTPClient: session.client,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer conn.Close(websocket.StatusNormalClosure, "")
	defer cancel()
	conn.SetReadLimit(8 << 20)
	pong := func() error {
		return wsWriteJSON(ctx, conn, map[string]any{"event": "pong"})
	}
	_, firstPayload, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	err = session.handle(firstPayload, pong)
	if err != nil {
		return err
	}
	session.connected.Store(true)
	defer session.connected.Store(false)
	s.logger.Info("websocket connected: ", session.name)
	messageCh := make(chan []byte)
	errCh := make(chan error, 1)
	go func() {
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case messageCh <- payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	statusTicker := time.NewTicker(webSocketStatusInterval)
	defer statusTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case payload := <-messageCh:
			err = session.handle(payload, pong)
			if err != nil {
				return err
			}
		case <-statusTicker.C:
			for _, status := range session.status() {
				err = wsWriteJSON(ctx, conn, map[string]any{
					"event":     "node.status",
					"data":      status,
					"timestamp": time.Now().Unix(),
				})
				if err != nil {
					return err
				}
			}
		}
	}
}
