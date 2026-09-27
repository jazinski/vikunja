// Vikunja is a to-do list application to facilitate your life.
// Copyright 2018-present Vikunja and contributors. All rights reserved.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package websocket

import (
	"sync"

	"code.vikunja.io/api/pkg/log"
	"code.vikunja.io/api/pkg/models"
	"code.vikunja.io/api/pkg/user"

	"xorm.io/xorm"
)

// Hub maintains the set of active connections and delivers messages to them.
type Hub struct {
	mu          sync.RWMutex
	connections map[int64][]*Connection // userID -> connections
}

// NewHub creates a new Hub.
func NewHub() *Hub {
	return &Hub{
		connections: make(map[int64][]*Connection),
	}
}

// Register adds a connection to the hub.
func (h *Hub) Register(conn *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connections[conn.userID] = append(h.connections[conn.userID], conn)
	log.Debugf("WebSocket: registered connection for user %d (total: %d)", conn.userID, len(h.connections[conn.userID]))
}

// Unregister removes a connection from the hub.
func (h *Hub) Unregister(conn *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	conns := h.connections[conn.userID]
	for i, c := range conns {
		if c == conn {
			h.connections[conn.userID] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	remaining := len(h.connections[conn.userID])
	if remaining == 0 {
		delete(h.connections, conn.userID)
	}
	log.Debugf("WebSocket: unregistered connection for user %d (remaining: %d)", conn.userID, remaining)
}

// PublishForUser sends an event to all connections of a specific user that are subscribed to the given event.
func (h *Hub) PublishForUser(userID int64, event string, data any) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	conns := h.connections[userID]
	msg := OutgoingMessage{
		Event: event,
		Data:  data,
	}

	for _, conn := range conns {
		if !conn.IsSubscribed(event) {
			continue
		}
		conn.trySend(msg)
	}
}

// PublishTaskEvent sends a task event to every subscribed connection whose
// user can read the task's project. Delivery is scoped per connection because
// subscribers of a task event are not known in advance: any authenticated user
// may subscribe, and project access differs per user.
func (h *Hub) PublishTaskEvent(s *xorm.Session, event string, task *models.Task) {
	h.mu.RLock()
	conns := h.allConnections()
	h.mu.RUnlock()

	access := newTaskAccess(s, task)
	msg := OutgoingMessage{Event: event, Data: task}
	for _, conn := range conns {
		if !conn.IsSubscribed(event) {
			continue
		}
		if !access.canRead(conn.UserID()) {
			continue
		}
		conn.trySend(msg)
	}
}

// PublishCommentEvent sends a task comment event to every subscribed
// connection whose user can read the comment's task. See PublishTaskEvent.
func (h *Hub) PublishCommentEvent(s *xorm.Session, event string, task *models.Task, payload *CommentEventPayload) {
	h.mu.RLock()
	conns := h.allConnections()
	h.mu.RUnlock()

	access := newTaskAccess(s, task)
	msg := OutgoingMessage{Event: event, Data: payload}
	for _, conn := range conns {
		if !conn.IsSubscribed(event) {
			continue
		}
		if !access.canRead(conn.UserID()) {
			continue
		}
		conn.trySend(msg)
	}
}

// allConnections returns every registered connection. Callers must hold h.mu.
func (h *Hub) allConnections() []*Connection {
	conns := make([]*Connection, 0, len(h.connections))
	for _, userConns := range h.connections {
		conns = append(conns, userConns...)
	}
	return conns
}

// taskAccess decides, for one fan-out, which users may receive an event about
// a task. It is short-lived: one instance per Publish* call, never shared
// between events, so its memo needs no locking.
type taskAccess struct {
	s     *xorm.Session
	task  *models.Task
	cache map[int64]bool
}

func newTaskAccess(s *xorm.Session, task *models.Task) *taskAccess {
	return &taskAccess{s: s, task: task, cache: make(map[int64]bool)}
}

// canRead reports whether the user may read the task, memoized per fan-out so
// N connections of the same user (or users resolving through the same project
// ACL) do not turn into N database roundtrips.
func (a *taskAccess) canRead(userID int64) bool {
	if allowed, ok := a.cache[userID]; ok {
		return allowed
	}
	allowed := a.check(userID)
	a.cache[userID] = allowed
	return allowed
}

func (a *taskAccess) check(userID int64) bool {
	// A fresh minimal task keeps the check honest: resolving from the event
	// payload would trust the task's own ProjectID, which every producer
	// sets correctly, but a copy costs little and guards against listeners
	// wired to events whose payload is user-controlled.
	canRead, _, err := (&models.Task{ID: a.task.ID}).CanRead(a.s, &user.User{ID: userID})
	switch {
	case err == nil:
		return canRead
	case models.IsErrTaskDoesNotExist(err):
		// task.deleted arrives after the soft delete: the row is invisible
		// to the refetch above, so scope by the event's project instead.
		// The event was dispatched server-side about a task that existed,
		// which is proof enough the ProjectID is genuine.
		canRead, _, err := (&models.Project{ID: a.task.ProjectID}).CanRead(a.s, &user.User{ID: userID})
		if err != nil {
			log.Errorf("WebSocket: access check for deleted task %d via project %d failed: %v", a.task.ID, a.task.ProjectID, err)
			return false
		}
		return canRead
	default:
		log.Errorf("WebSocket: access check for task %d failed: %v", a.task.ID, err)
		return false
	}
}

// trySend enqueues msg without ever blocking: a slow consumer must not hold
// up fan-out to other connections.
func (c *Connection) trySend(msg OutgoingMessage) {
	select {
	case c.send <- msg:
	default:
		log.Warningf("WebSocket: send buffer full for user %d, dropping message", c.UserID())
	}
}
