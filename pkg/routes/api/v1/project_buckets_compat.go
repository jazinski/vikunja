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

package v1

import (
	"net/http"
	"strconv"

	"code.vikunja.io/api/pkg/db"
	"code.vikunja.io/api/pkg/log"
	"code.vikunja.io/api/pkg/models"
	"code.vikunja.io/api/pkg/modules/auth"

	"github.com/labstack/echo/v4"
)

// GetProjectBuckets is a compatibility alias for the pre-view kanban API.
// Stable v1 (<= v0.22) exposed a project's kanban buckets directly at
// /projects/{project}/buckets. The views refactor moved buckets under
// /projects/{project}/views/{view}/buckets, which left API clients that
// enumerate buckets by project with a 404. This handler resolves the
// project's kanban view at runtime — bucket ids are per-project-view and
// must never be hardcoded by clients — and returns its buckets with the
// is_done_bucket flag set, so automation can find the Done column without
// a second round-trip.
//
// @Summary Get a project's kanban buckets (compat)
// @Description Returns all kanban buckets of the project's kanban view. Compat alias for the pre-views API: resolves the kanban view at runtime and returns its buckets. Use /projects/{project}/views/{view}/buckets for full control.
// @tags project
// @Accept json
// @Produce json
// @Security JWTKeyAuth
// @Param project path int true "Project ID"
// @Success 200 {array} models.Bucket "The buckets"
// @Failure 403 {object} web.HTTPError "No access to the project"
// @Failure 404 {object} models.Message "The project has no kanban view"
// @Failure 500 {object} models.Message "Internal error"
// @Router /projects/{project}/buckets [get]
func GetProjectBuckets(c echo.Context) error {
	projectID, err := strconv.ParseInt(c.Param("project"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid project id.")
	}

	s := db.NewSession()
	defer s.Close()

	currentAuth, err := auth.GetAuthFromClaims(&c)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Could not determine the current user.").Wrap(err)
	}

	buckets, err := models.GetKanbanBucketsForProject(s, projectID, currentAuth)
	if err != nil {
		return handlerError(err)
	}
	return c.JSON(http.StatusOK, buckets)
}

func handlerError(err error) error {
	log.Debugf("compat buckets handler: %s", err)
	if models.IsErrProjectDoesNotExist(err) {
		return echo.NewHTTPError(http.StatusNotFound, "The project does not exist.")
	}
	return echo.NewHTTPError(http.StatusInternalServerError, "Internal error.").Wrap(err)
}
