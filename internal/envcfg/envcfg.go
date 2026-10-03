// Package envcfg reads typed configuration from environment variables.
package envcfg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns the variable or def when unset.
func String(name, def string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return def
}

// Required returns the variable or an error when unset.
func Required(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return v, nil
}

// Int parses an integer variable.
func Int(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return n
	}
	return def
}

// Float parses a float variable.
func Float(name string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil {
		return f
	}
	return def
}

// Duration parses a Go duration such as "2s" or "150ms".
func Duration(name string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(name)); err == nil {
		return d
	}
	return def
}

// Tokens parses "token:actor,token2:actor2" into a map.
func Tokens(name string) (map[string]string, error) {
	raw, err := Required(name)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		token, actor, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok || len(token) < 16 || actor == "" {
			return nil, fmt.Errorf("%s: each entry must be token:actor with a token of 16+ chars", name)
		}
		out[token] = actor
	}
	return out, nil
}
