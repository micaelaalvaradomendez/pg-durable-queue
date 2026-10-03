// Package upstream is the HTTP client for the external warehouse-assignment API.
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ErrorKind tells the worker whether retrying can help.
type ErrorKind int

const (
	Transient ErrorKind = iota
	Permanent
)

// Error is returned for any non-successful call.
type Error struct {
	Kind   ErrorKind
	Status int
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream: status=%d: %s", e.Status, e.Msg)
}

// IsPermanent reports whether err will not succeed on retry.
func IsPermanent(err error) bool {
	var ue *Error
	return errors.As(err, &ue) && ue.Kind == Permanent
}

// Client calls the upstream API. Every assignment carries an Idempotency-Key so
// that replaying a request whose response was lost never applies it twice.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient builds a client with a per-request timeout.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout:   timeout,
			Transport: &http.Transport{DisableKeepAlives: true},
		},
	}
}

type assignRequest struct {
	OrderRef  string `json:"order_ref"`
	Warehouse string `json:"warehouse"`
}

// Assign asks upstream to move orderRef to warehouse.
func (c *Client) Assign(ctx context.Context, idempotencyKey, orderRef, warehouse string) error {
	body, err := json.Marshal(assignRequest{OrderRef: orderRef, Warehouse: warehouse})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/assignments", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return &Error{Kind: Transient, Msg: err.Error()}
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return classify(resp.StatusCode, string(msg))
}

// Applied asks upstream whether the assignment with this key already took effect.
// It is used after an ambiguous failure (timeout, dropped connection).
func (c *Client) Applied(ctx context.Context, idempotencyKey string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/assignments/"+url.PathEscape(idempotencyKey), nil)
	if err != nil {
		return false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, &Error{Kind: Transient, Msg: err.Error()}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, classify(resp.StatusCode, "status lookup failed")
	}
}

func classify(status int, msg string) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusTooManyRequests, status == http.StatusRequestTimeout, status >= 500:
		return &Error{Kind: Transient, Status: status, Msg: msg}
	default:
		return &Error{Kind: Permanent, Status: status, Msg: msg}
	}
}
