package service

import (
	"context"
	"net"
	"net/http"
)

// RequestBaseURL returns the base URL ("http://host:port") of the kumo
// listener that accepted the request ctx belongs to, for services that call
// back into kumo itself, such as EventBridge delivering to an SQS target.
//
// The address is the listener's own end of the connection, so the call
// reaches the same server whatever port it was started on: a random port from
// kumo.NewServer, a --port flag, or a container port published elsewhere.
// It returns fallback when ctx carries no listener address, for example when
// the call does not come from an HTTP request.
func RequestBaseURL(ctx context.Context, fallback string) string {
	addr, ok := ctx.Value(http.LocalAddrContextKey).(*net.TCPAddr)
	if !ok || addr == nil || addr.IP.IsUnspecified() {
		return fallback
	}

	return "http://" + addr.String()
}
