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

package webtests

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"code.vikunja.io/api/pkg/db"
	"code.vikunja.io/api/pkg/models"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the kanban write bugs (#42 on the casa board):
// bucket_id sent to the generic task update silently vanished, and any
// generic update without an assignees field wiped existing assignees.

func kanbanReq(e *echo.Echo, method, target, jwt, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(echo.HeaderAuthorization, "Bearer "+jwt)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	res := httptest.NewRecorder()
	e.ServeHTTP(res, req)
	return res
}

func TestKanbanWritesRegression(t *testing.T) {
	// Fixture facts (pkg/db/fixtures):
	//   view 4 = kanban view of project 1, done_bucket_id: 3,
	//            default_bucket_id: 1
	//   bucket 1 = "testbucket1" (backlog-ish), bucket 3 = done bucket
	//   task 1 is in bucket 1 (view 4), task 2 is in bucket 3 (done)
	//   task 30 has assignee user 1 (task_assignees fixture id 1)

	t.Run("generic task update persists bucket_id (db assertion)", func(t *testing.T) {
		e, err := setupTestEnv()
		require.NoError(t, err)

		res := kanbanReq(e, http.MethodPost, "/api/v1/tasks/1", userJWT(t, 1), `{"bucket_id":2}`)
		require.Equal(t, http.StatusOK, res.Code)

		s := db.NewSession()
		defer s.Close()
		db.AssertExists(t, "task_buckets", map[string]interface{}{
			"task_id":         1,
			"project_view_id": 4,
			"bucket_id":       2,
		}, false)
	})

	t.Run("generic task update into done bucket marks the task done", func(t *testing.T) {
		e, err := setupTestEnv()
		require.NoError(t, err)

		// Task 1 is not done; moving it into the done bucket (3) via the
		// generic endpoint must flip done, same as a UI drag would.
		res := kanbanReq(e, http.MethodPost, "/api/v1/tasks/1", userJWT(t, 1), `{"bucket_id":3}`)
		require.Equal(t, http.StatusOK, res.Code)

		s := db.NewSession()
		defer s.Close()
		db.AssertExists(t, "tasks", map[string]interface{}{
			"id":   1,
			"done": true,
		}, false)
		db.AssertExists(t, "task_buckets", map[string]interface{}{
			"task_id":         1,
			"project_view_id": 4,
			"bucket_id":       3,
		}, false)
	})

	t.Run("generic task update without assignees keeps them", func(t *testing.T) {
		e, err := setupTestEnv()
		require.NoError(t, err)

		// Task 30 has assignee user 1. A partial update that does not
		// mention assignees at all must not unassign them.
		res := kanbanReq(e, http.MethodPost, "/api/v1/tasks/30", userJWT(t, 1), `{"priority":5}`)
		require.Equal(t, http.StatusOK, res.Code)

		s := db.NewSession()
		defer s.Close()
		count, err := s.Where("task_id = ?", 30).Count(&models.TaskAssginee{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count, "assignee must survive an update that does not send assignees")
	})

	t.Run("explicit empty assignees list still clears assignees", func(t *testing.T) {
		e, err := setupTestEnv()
		require.NoError(t, err)

		res := kanbanReq(e, http.MethodPost, "/api/v1/tasks/30", userJWT(t, 1), `{"assignees":[]}`)
		require.Equal(t, http.StatusOK, res.Code)

		s := db.NewSession()
		defer s.Close()
		count, err := s.Where("task_id = ?", 30).Count(&models.TaskAssginee{})
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "explicit empty list must clear assignees")
	})

	t.Run("project buckets compat alias lists buckets with done flag", func(t *testing.T) {
		e, err := setupTestEnv()
		require.NoError(t, err)

		res := kanbanReq(e, http.MethodGet, "/api/v1/projects/1/buckets", userJWT(t, 1), "")
		require.Equal(t, http.StatusOK, res.Code)

		// Bucket 3 is the done bucket of view 4 — resolved at runtime,
		// no hardcoded view id involved.
		assert.Contains(t, res.Body.String(), `"is_done_bucket":true`)
		assert.Contains(t, res.Body.String(), `"id":3`)
	})

	t.Run("project buckets compat alias denies strangers", func(t *testing.T) {
		e, err := setupTestEnv()
		require.NoError(t, err)

		// User 2 has no access to project 1 in the fixtures.
		res := kanbanReq(e, http.MethodGet, "/api/v1/projects/1/buckets", userJWT(t, 2), "")
		assert.Equal(t, http.StatusForbidden, res.Code)
	})
}
