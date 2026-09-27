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
	"encoding/json"
	"sync"
	"testing"

	"code.vikunja.io/api/pkg/db"
	"code.vikunja.io/api/pkg/events"
	"code.vikunja.io/api/pkg/files"
	"code.vikunja.io/api/pkg/models"
	"code.vikunja.io/api/pkg/user"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm"
)

// Only this test needs a database, so the engine is built lazily instead of in
// TestMain where it would slow down every other test in the package.
var taskListenerDBOnce sync.Once

// Fixtures: project 1 belongs to user1; project 3 is shared with user1 and
// user2 directly and with team 1 (user1). user2 has no access to project 1.
func TestTaskEventsListener(t *testing.T) {
	t.Run("pushes task.created to subscribed users who can read the project", func(t *testing.T) {
		s := setupTaskListenerTest(t)
		InitHub()
		owner := taskConn(1, 3)
		member := taskConn(2, 3)
		GetHub().Register(owner)
		GetHub().Register(member)

		handleTaskEvent(t, s, &models.TaskCreatedEvent{
			Task: &models.Task{ID: 32, ProjectID: 3},
			Doer: &user.User{ID: 1},
		})

		for _, conn := range []*Connection{owner, member} {
			require.Len(t, conn.send, 1)
			msg := <-conn.send
			assert.Equal(t, "project.3.tasks", msg.Event)
			payload, ok := msg.Data.(*TaskEventPayload)
			require.True(t, ok, "payload must be a TaskEventPayload")
			assert.Equal(t, "task.created", payload.Type)
			assert.Equal(t, int64(32), payload.TaskID)
			assert.Equal(t, int64(3), payload.ProjectID)
			assert.Equal(t, int64(1), payload.DoerID)
		}
	})

	t.Run("does not push to a user without read access to the project", func(t *testing.T) {
		s := setupTaskListenerTest(t)
		InitHub()
		outsider := taskConn(2, 1) // project 1 is user1's; user2 has no share
		GetHub().Register(outsider)

		handleTaskEvent(t, s, &models.TaskCreatedEvent{
			Task: &models.Task{ID: 10, ProjectID: 1},
			Doer: &user.User{ID: 1},
		})

		assert.Empty(t, outsider.send)
	})

	t.Run("stops pushing once project access is revoked", func(t *testing.T) {
		s := setupTaskListenerTest(t)
		InitHub()
		conn := taskConn(1, 3)
		GetHub().Register(conn)

		unshareProject(t, s, 3, 1)
		handleTaskEvent(t, s, &models.TaskCreatedEvent{
			Task: &models.Task{ID: 32, ProjectID: 3},
			Doer: &user.User{ID: 2},
		})

		assert.Empty(t, conn.send)
	})

	t.Run("does not push to a subscribed user of a different project channel", func(t *testing.T) {
		s := setupTaskListenerTest(t)
		InitHub()
		other := taskConn(1, 1) // subscribed to project.1.tasks, event is on project 3
		GetHub().Register(other)

		handleTaskEvent(t, s, &models.TaskCreatedEvent{
			Task: &models.Task{ID: 32, ProjectID: 3},
			Doer: &user.User{ID: 1},
		})

		assert.Empty(t, other.send)
	})

	t.Run("pushes task.updated and task.comment.created with their own type", func(t *testing.T) {
		s := setupTaskListenerTest(t)
		InitHub()
		conn := taskConn(1, 3)
		GetHub().Register(conn)

		handleTaskEvent(t, s, &models.TaskUpdatedEvent{
			Task: &models.Task{ID: 32, ProjectID: 3},
			Doer: &user.User{ID: 2},
		})
		require.Len(t, conn.send, 1)
		msg := <-conn.send
		payload, ok := msg.Data.(*TaskEventPayload)
		require.True(t, ok)
		assert.Equal(t, "task.updated", payload.Type)

		handleTaskEvent(t, s, &models.TaskCommentCreatedEvent{
			Task:    &models.Task{ID: 32, ProjectID: 3},
			Comment: &models.TaskComment{ID: 5},
			Doer:    &user.User{ID: 2},
		})
		require.Len(t, conn.send, 1)
		msg = <-conn.send
		payload, ok = msg.Data.(*TaskEventPayload)
		require.True(t, ok)
		assert.Equal(t, "task.comment.created", payload.Type)
	})
}

func TestProjectChannel(t *testing.T) {
	assert.Equal(t, "project.3.tasks", ProjectChannel(3))
}

func TestIsValidEventProjectChannels(t *testing.T) {
	assert.True(t, isValidEvent("notification.created"))
	assert.True(t, isValidEvent("project.1.tasks"))
	assert.True(t, isValidEvent("project.999.tasks"))
	assert.False(t, isValidEvent("project.0.tasks"))
	assert.False(t, isValidEvent("project.-1.tasks"))
	assert.False(t, isValidEvent("project.abc.tasks"))
	assert.False(t, isValidEvent("project.1"))
	assert.False(t, isValidEvent("project.1.comments"))
	assert.False(t, isValidEvent("project.1.tasks.extra"))
	assert.False(t, isValidEvent("project..tasks"))
	assert.False(t, isValidEvent("other.1.tasks"))
}

func TestHubSubscribedUserIDs(t *testing.T) {
	h := NewHub()
	c1 := taskConn(1, 3)
	c2 := taskConn(1, 1)
	c3 := taskConn(2, 3)
	h.Register(c1)
	h.Register(c2)
	h.Register(c3)

	assert.ElementsMatch(t, []int64{1, 2}, h.SubscribedUserIDs(ProjectChannel(3)))
	assert.ElementsMatch(t, []int64{1}, h.SubscribedUserIDs(ProjectChannel(1)))
	assert.Empty(t, h.SubscribedUserIDs(ProjectChannel(2)))
}

func setupTaskListenerTest(t *testing.T) *xorm.Session {
	t.Helper()
	taskListenerDBOnce.Do(func() {
		// The fixture loader cleans every registered table, so files and users
		// have to be set up before models.
		files.InitTests()
		user.InitTests()
		models.SetupTests()
	})

	db.LoadAndAssertFixtures(t)
	s := db.NewSession()
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func taskConn(userID, projectID int64) *Connection {
	return &Connection{
		userID:        userID,
		subscriptions: map[string]bool{ProjectChannel(projectID): true},
		send:          make(chan OutgoingMessage, 16),
	}
}

// handleTaskEvent runs the matching listener for the event. Commits first:
// the listener opens its own session, so anything still inside the test
// transaction is invisible to it.
func handleTaskEvent(t *testing.T, s *xorm.Session, event events.Event) {
	t.Helper()
	require.NoError(t, s.Commit())

	content, err := json.Marshal(event)
	require.NoError(t, err)

	var l *TaskEventsListener
	switch event.(type) {
	case *models.TaskCreatedEvent:
		l = &TaskEventsListener{wsEvent: "task.created"}
	case *models.TaskUpdatedEvent:
		l = &TaskEventsListener{wsEvent: "task.updated"}
	case *models.TaskCommentCreatedEvent:
		l = &TaskEventsListener{wsEvent: "task.comment.created"}
	}
	require.NotNil(t, l, "no listener mapped for %s", event.Name())
	require.NoError(t, l.Handle(message.NewMessage("test", content)))
}
