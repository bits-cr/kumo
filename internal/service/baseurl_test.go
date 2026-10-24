package service

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestBaseURL(t *testing.T) {
	t.Parallel()

	const fallback = "http://localhost:4566"

	tests := []struct {
		name string
		ctx  context.Context //nolint:containedctx // table input
		want string
	}{
		{
			name: "no listener address",
			ctx:  context.Background(),
			want: fallback,
		},
		{
			name: "IPv4 listener",
			ctx:  context.WithValue(context.Background(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321}),
			want: "http://127.0.0.1:54321",
		},
		{
			name: "IPv6 listener",
			ctx:  context.WithValue(context.Background(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv6loopback, Port: 4566}),
			want: "http://[::1]:4566",
		},
		{
			name: "unspecified address",
			ctx:  context.WithValue(context.Background(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4zero, Port: 4566}),
			want: fallback,
		},
		{
			name: "non-TCP address",
			ctx:  context.WithValue(context.Background(), http.LocalAddrContextKey, &net.UnixAddr{Name: "/tmp/kumo.sock", Net: "unix"}),
			want: fallback,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := RequestBaseURL(tt.ctx, fallback); got != tt.want {
				t.Errorf("RequestBaseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRequestBaseURL_Server checks the address net/http attaches to a real
// request: the URL must be the server's own.
func TestRequestBaseURL_Server(t *testing.T) {
	t.Parallel()

	got := make(chan string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got <- RequestBaseURL(r.Context(), "http://localhost:4566")
	}))
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	_ = resp.Body.Close()

	if url := <-got; url != srv.URL {
		t.Errorf("RequestBaseURL() = %q, want the server's URL %q", url, srv.URL)
	}
}
