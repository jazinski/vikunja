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
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package websocket

import (
	"context"
	"sync"
	"testing"

	"code.vikunja.io/api/pkg/db"
	"code.vikunja.io/api/pkg/files"
	"code.vikunja.io/api/pkg/models"
	"code.vikunja.io/api/pkg/user"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var authDBOnce sync.Once

func setupAuthDBTest(t *testing.T) {
	t.Helper()
	authDBOnce.Do(func() {
		// The fixture loader cleans every registered table, so files and users
		// have to be set up before models.
		files.InitTests()
		user.InitTests()
		models.SetupTests()
	})
	db.LoadAndAssertFixtures(t)
}

// resolveAuthToken is tested directly for the failure paths because
// handleAuth writes the error straight to the websocket before returning,
// which needs a live connection.
func TestResolveAuthTokenAcceptsAPIToken(t *testing.T) {
	setupAuthDBTest(t)

	// Fixture token 1: valid until 2099, owned by user 1.
	userID, err := resolveAuthToken("tk_2eef46f40ebab3304919ab2e7e39993f75f29d2e")
	require.NoError(t, err)
	assert.Equal(t, int64(1), userID)
}

func TestResolveAuthTokenRejectsBadTokens(t *testing.T) {
	setupAuthDBTest(t)

	// Fixture token 2: expired 2023-01-01, owned by user 1.
	_, err := resolveAuthToken("tk_a5e6f92ddbad68f49ee2c63e52174db0235008c8")
	assert.Error(t, err, "expired API token must be rejected")

	// Well-formed but unknown token.
	_, err = resolveAuthToken("tk_0000000000000000000000000000000000000000")
	assert.Error(t, err, "unknown API token must be rejected")

	// Fixture token 4: valid, but its owner account is disabled.
	_, err = resolveAuthToken("tk_disabled_user_test_token_000000001234abcd")
	assert.Error(t, err, "API token of a disabled user must be rejected")

	for _, token := range []string{"", "garbage", "Bearer tk_2eef46f40ebab3304919ab2e7e39993f75f29d2e"} {
		_, err = resolveAuthToken(token)
		assert.Error(t, err, "token %q must not authenticate", token)
	}
}

// The full auth flow with an API token, including the hub registration and
// the auth.success reply. Invalid tokens close the connection after a direct
// error write (see handleAuth), so they are covered by TestResolveAuthToken*.
func TestHandleAuthWithAPIToken(t *testing.T) {
	setupAuthDBTest(t)

	hub := NewHub()
	conn := &Connection{
		hub:           hub,
		subscriptions: make(map[string]bool),
		send:          make(chan OutgoingMessage, 16),
	}

	ok := conn.handleAuth(context.Background(), "tk_2eef46f40ebab3304919ab2e7e39993f75f29d2e")
	require.True(t, ok, "valid API token must authenticate")
	assert.True(t, conn.IsAuthenticated())
	assert.Equal(t, int64(1), conn.UserID())

	msg := <-conn.send
	assert.Equal(t, ActionAuthSuccess, msg.Action)
	assert.True(t, msg.Success)

	// An API-token-authenticated connection must behave like any other:
	// subscribe and show up in the hub.
	conn.handleMessage(context.Background(), IncomingMessage{Action: ActionSubscribe, Event: "notification.created"})
	assert.True(t, conn.IsSubscribed("notification.created"))
	assert.ElementsMatch(t, []int64{1}, hub.SubscribedUserIDs("notification.created"))
}
