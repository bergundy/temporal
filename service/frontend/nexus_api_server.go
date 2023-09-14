package frontend

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/encryption"
	"google.golang.org/grpc"
)

// NexusAPIServer is an HTTP API server that translates Nexus HTTP requests to Nexus tasks that are dispatched to
// workers via matching.
type NexusAPIServer struct {
	server   http.Server
	listener net.Listener
	logger   log.Logger
	stopped  chan struct{}
}

// NewNexusAPIServer creates a [NexusAPIServer].
func NewNexusAPIServer(
	serviceConfig *Config,
	rpcConfig config.RPC,
	grpcListener net.Listener,
	tlsConfigProvider encryption.TLSConfigProvider,
	interceptors []grpc.UnaryServerInterceptor,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) (*HTTPAPIServer, error) {
	// Create a TCP listener the same as the frontend one but with different port
	tcpAddrRef, _ := grpcListener.Addr().(*net.TCPAddr)
	if tcpAddrRef == nil {
		return nil, errHTTPGRPCListenerNotTCP
	}
	tcpAddr := *tcpAddrRef
	tcpAddr.Port = rpcConfig.HTTPPort
	var listener net.Listener
	var err error
	if listener, err = net.ListenTCP("tcp", &tcpAddr); err != nil {
		return nil, fmt.Errorf("failed listening for HTTP API on %v: %w", &tcpAddr, err)
	}
	// Close the listener if anything else in this function fails
	success := false
	defer func() {
		if !success {
			_ = listener.Close()
		}
	}()

	// Wrap the listener in a TLS listener if there is any TLS config
	if tlsConfigProvider != nil {
		if tlsConfig, err := tlsConfigProvider.GetFrontendServerConfig(); err != nil {
			return nil, fmt.Errorf("failed getting TLS config for HTTP API: %w", err)
		} else if tlsConfig != nil {
			listener = tls.NewListener(listener, tlsConfig)
		}
	}

	h := &HTTPAPIServer{
		listener: listener,
		logger:   logger,
		stopped:  make(chan struct{}),
	}

	handler := nexus.NewHTTPHandler(nexus.HandlerOptions{
		Handler:          &nexus.UnimplementedHandler{},
		GetResultTimeout: serviceConfig.KeepAliveMaxConnectionIdle(),
		// Logger: ,
	})
	h.server.Handler = handler

	// Put the remote address on the context
	h.server.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		return context.WithValue(ctx, httpRemoteAddrContextKey{}, c)
	}

	// We want to set ReadTimeout and WriteTimeout as max idle (and IdleTimeout
	// defaults to ReadTimeout) to ensure that a connection cannot hang over that
	// amount of time.
	h.server.ReadTimeout = serviceConfig.KeepAliveMaxConnectionIdle()
	h.server.WriteTimeout = serviceConfig.KeepAliveMaxConnectionIdle()

	success = true
	return h, nil
}

// // Serve serves the HTTP API and does not return until there is a serve error or
// // GracefulStop completes. Upon graceful stop, this will return nil. If an error
// // is returned, the message is clear that it came from the HTTP API server.
// func (h *HTTPAPIServer) Serve() error {
// 	err := h.server.Serve(h.listener)
// 	// If the error is for close, we have to wait for the shutdown to complete and
// 	// we don't consider it an error
// 	if errors.Is(err, http.ErrServerClosed) {
// 		<-h.stopped
// 		err = nil
// 	}
// 	// Wrap the error to be clearer it's from the HTTP API
// 	if err != nil {
// 		return fmt.Errorf("HTTP API serve failed: %w", err)
// 	}
// 	return nil
// }

// // GracefulStop stops the HTTP server. This will first attempt a graceful stop
// // with a drain time, then will hard-stop. This will not return until stopped.
// func (h *HTTPAPIServer) GracefulStop(gracefulDrainTime time.Duration) {
// 	// We try a graceful stop for the amount of time we can drain, then we do a
// 	// hard stop
// 	shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulDrainTime)
// 	defer cancel()
// 	// We intentionally ignore this error, we're gonna stop at this point no
// 	// matter what. This closes the listener too.
// 	_ = h.server.Shutdown(shutdownCtx)
// 	_ = h.server.Close()
// 	close(h.stopped)
// }

// func (h *HTTPAPIServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
// 	// Limit the request body to max gRPC size. This is hardcoded to 4MB at the
// 	// moment using gRPC's default at
// 	// https://github.com/grpc/grpc-go/blob/0673105ebcb956e8bf50b96e28209ab7845a65ad/server.go#L58
// 	// which is what the constant is set as at the time of this comment.
// 	r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxHTTPAPIRequestBytes)

// 	h.logger.Debug(
// 		"HTTP API call",
// 		tag.NewStringTag("http-method", r.Method),
// 		tag.NewAnyTag("http-url", r.URL),
// 	)

// 	// Need to change the accept header based on whether pretty and/or
// 	// noPayloadShorthand are present
// 	var acceptHeaderSuffix string
// 	if _, ok := r.URL.Query()["pretty"]; ok {
// 		acceptHeaderSuffix += "+pretty"
// 	}
// 	if _, ok := r.URL.Query()["noPayloadShorthand"]; ok {
// 		acceptHeaderSuffix += "+no-payload-shorthand"
// 	}
// 	if acceptHeaderSuffix != "" {
// 		r.Header.Set("Accept", "application/json"+acceptHeaderSuffix)
// 	}

// 	// Put the TLS info on the peer context
// 	if r.TLS != nil {
// 		var addr net.Addr
// 		if conn, _ := r.Context().Value(httpRemoteAddrContextKey{}).(net.Conn); conn != nil {
// 			addr = conn.RemoteAddr()
// 		}
// 		r = r.WithContext(peer.NewContext(r.Context(), &peer.Peer{
// 			Addr: addr,
// 			AuthInfo: credentials.TLSInfo{
// 				State:          *r.TLS,
// 				CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
// 			},
// 		}))
// 	}

// 	// Call gRPC gateway mux
// 	h.serveMux.ServeHTTP(w, r)
// }
