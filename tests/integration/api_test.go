package integration

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/httpapi"
)

const testToken = "test-token-0123456789"

func newAPI(t *testing.T) *httptest.Server {
	t.Helper()
	st := openStore(t)
	api := &httpapi.API{
		Store:  st,
		Tokens: map[string]string{testToken: "operator-ana"},
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path, token, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestAPIRequiresAuthentication(t *testing.T) {
	srv := newAPI(t)
	if code, _ := call(t, srv, "POST", "/sessions/session-01/scans", "", `{"order_ref":"ORD-0001"}`, nil); code != http.StatusUnauthorized {
		t.Fatalf("no token: status %d", code)
	}
	if code, _ := call(t, srv, "POST", "/sessions/session-01/scans", "wrong-token-000000000", `{"order_ref":"ORD-0001"}`, nil); code != http.StatusUnauthorized {
		t.Fatalf("bad token: status %d", code)
	}
}

func TestAPIConfirmFlowAndIdempotency(t *testing.T) {
	srv := newAPI(t)
	for _, ref := range []string{"ORD-0001", "ORD-0002", "ORD-0003"} {
		if code, _ := call(t, srv, "POST", "/sessions/session-01/scans", testToken, `{"order_ref":"`+ref+`"}`, nil); code != http.StatusCreated {
			t.Fatalf("scan %s: status %d", ref, code)
		}
	}
	if code, _ := call(t, srv, "POST", "/sessions/session-01/scans", testToken, `{"order_ref":"bad ref"}`, nil); code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid order ref: status %d", code)
	}

	body := `{"session_id":"session-01","target_warehouse":"WH-NORTH"}`
	if code, _ := call(t, srv, "POST", "/batches", testToken, body, nil); code != http.StatusBadRequest {
		t.Fatalf("missing Idempotency-Key: status %d", code)
	}
	hdr := map[string]string{"Idempotency-Key": "confirm-0001"}
	code, first := call(t, srv, "POST", "/batches", testToken, body, hdr)
	if code != http.StatusCreated || first["created_by"] != "operator-ana" || first["item_count"] != float64(3) {
		t.Fatalf("confirm: status %d body %v", code, first)
	}
	code, replay := call(t, srv, "POST", "/batches", testToken, body, hdr)
	if code != http.StatusOK || replay["id"] != first["id"] {
		t.Fatalf("replay: status %d body %v", code, replay)
	}

	code, progress := call(t, srv, "GET", "/batches/"+first["id"].(string), testToken, "", nil)
	if code != http.StatusOK || progress["done"] != false {
		t.Fatalf("progress: status %d body %v", code, progress)
	}
	if code, _ := call(t, srv, "POST", "/batches", testToken, `{"session_id":"session-01","target_warehouse":"WH-NORTH","actor":"admin"}`,
		map[string]string{"Idempotency-Key": "confirm-0002"}); code != http.StatusBadRequest {
		t.Fatalf("unknown fields (actor spoofing) must be rejected: status %d", code)
	}
}
