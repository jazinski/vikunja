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

	"code.vikunja.io/api/pkg/db"
	"code.vikunja.io/api/pkg/events"
	"code.vikunja.io/api/pkg/log"
	"code.vikunja.io/api/pkg/models"
	"code.vikunja.io/api/pkg/user"

	"github.com/ThreeDotsLabs/watermill/message"
)

// TaskEventPayload is the thin notification pushed on project.<id>.tasks.
// Clients refetch the affected view when they receive it (the task itself is
// deliberately not embedded: it would be stale by the time the client
// refetches, and skipping it keeps the push cheap and leak-free).
type TaskEventPayload struct {
	Type      string `json:"type"`
	TaskID    int64  `json:"task_id"`
	ProjectID int64  `json:"project_id"`
	DoerID    int64  `json:"doer_id,omitempty"`
}

// TaskEventsListener pushes task lifecycle events to the project channel.
type TaskEventsListener struct {
	// wsEvent is the data.type carried in the payload, e.g. "task.created".
	wsEvent string
}

func (l *TaskEventsListener) Name() string { return "websocket.push." + l.wsEvent }

func (l *TaskEventsListener) Handle(msg *message.Message) error {
	// Task events share the {task, doer} shape; the comment event adds
	// {comment}, which the push does not need.
	var event struct {
		Task *models.Task `json:"task"`
		Doer *user.User   `json:"doer"`
	}
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return err
	}
	if event.Task == nil {
		return nil
	}
	projectID := event.Task.ProjectID
	if projectID < 1 {
		return nil
	}

	hub := GetHub()
	if hub == nil {
		log.Warningf("WebSocket: hub not initialized, skipping task event push")
		return nil
	}

	doerID := int64(0)
	if event.Doer != nil {
		doerID = event.Doer.ID
	}
	payload := &TaskEventPayload{
		Type:      l.wsEvent,
		TaskID:    event.Task.ID,
		ProjectID: projectID,
		DoerID:    doerID,
	}

	// Access is checked per user at push time (same posture as the
	// notification listener): a share revoked after subscribing must stop
	// the stream immediately, so eligibility cannot be decided at
	// subscribe time.
	for _, userID := range hub.SubscribedUserIDs(ProjectChannel(projectID)) {
		s := db.NewSession()
		canRead, _, err := (&models.Project{ID: projectID}).CanRead(s, &user.User{ID: userID})
		_ = s.Close()
		if err != nil {
			log.Errorf("WebSocket: failed to check project %d access for user %d: %v", projectID, userID, err)
			continue
		}
		if !canRead {
			continue
		}
		hub.PublishForUser(userID, ProjectChannel(projectID), payload)
	}
	return nil
}

// RegisterTaskListeners registers the project-scoped task event listeners.
func RegisterTaskListeners() {
	events.RegisterListener((&models.TaskCreatedEvent{}).Name(), &TaskEventsListener{wsEvent: "task.created"})
	events.RegisterListener((&models.TaskUpdatedEvent{}).Name(), &TaskEventsListener{wsEvent: "task.updated"})
	events.RegisterListener((&models.TaskCommentCreatedEvent{}).Name(), &TaskEventsListener{wsEvent: "task.comment.created"})
}
