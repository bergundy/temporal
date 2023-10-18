package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
)

type nexusContextKey struct{}

type nexusContext struct {
	service   *persistence.Service
	namespace *namespace.Namespace
}

// NexusAPIServer is an HTTP API server that translates Nexus HTTP requests to Nexus tasks that are dispatched to
// workers via matching.
type NexusAPIServer struct {
	server            http.Server
	logger            log.Logger
	nexusHandler      http.Handler
	serviceRegistry   persistence.IncomingServiceRegistry
	namespaceRegistry namespace.Registry
	historyClient     historyservice.HistoryServiceClient
	listener          net.Listener
	stopped           chan struct{}
}

// NewNexusAPIServer creates a [NexusAPIServer].
func NewNexusAPIServer(
	serviceConfig *Config,
	rpcConfig config.RPC,
	grpcListener net.Listener,
	tlsConfigProvider encryption.TLSConfigProvider,
	metricsHandler metrics.Handler,
	serviceRegistry persistence.IncomingServiceRegistry,
	namespaceRegistry namespace.Registry,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	logger log.Logger,
) (*NexusAPIServer, error) {
	// Create a TCP listener the same as the frontend one but with different port
	tcpAddrRef, _ := grpcListener.Addr().(*net.TCPAddr)
	if tcpAddrRef == nil {
		return nil, errHTTPGRPCListenerNotTCP
	}
	tcpAddr := *tcpAddrRef
	tcpAddr.Port = rpcConfig.NexusPort
	var listener net.Listener
	var err error
	if listener, err = net.ListenTCP("tcp", &tcpAddr); err != nil {
		return nil, fmt.Errorf("failed listening for Nexus API on %v: %w", &tcpAddr, err)
	}
	// Close the listener if anything else in this function fails
	success := false
	defer func() {
		if !success {
			_ = listener.Close()
		}
	}()

	// // Wrap the listener in a TLS listener if there is any TLS config
	// if tlsConfigProvider != nil {
	// 	if tlsConfig, err := tlsConfigProvider.GetFrontendServerConfig(); err != nil {
	// 		return nil, fmt.Errorf("failed getting TLS config for Nexus API: %w", err)
	// 	} else if tlsConfig != nil {
	// 		listener = tls.NewListener(listener, tlsConfig)
	// 	}
	// }

	s := &NexusAPIServer{
		listener:          listener,
		logger:            logger,
		serviceRegistry:   serviceRegistry,
		namespaceRegistry: namespaceRegistry,
		historyClient:     historyClient,
		stopped:           make(chan struct{}),
	}

	handler := nexus.NewHTTPHandler(nexus.HandlerOptions{
		Handler:          &nexusHandler{logger: logger, matchingClient: matchingClient},
		GetResultTimeout: serviceConfig.KeepAliveMaxConnectionIdle(),
		Logger:           log.NewSlogLogger(logger),
	})
	// Set the handler as our function that wraps serve mux
	s.server.Handler = http.HandlerFunc(s.serveHTTP)
	s.nexusHandler = handler

	// Put the remote address on the context
	s.server.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		return context.WithValue(ctx, httpRemoteAddrContextKey{}, c)
	}

	// We want to set ReadTimeout and WriteTimeout as max idle (and IdleTimeout
	// defaults to ReadTimeout) to ensure that a connection cannot hang over that
	// amount of time.
	s.server.ReadTimeout = serviceConfig.KeepAliveMaxConnectionIdle()
	s.server.WriteTimeout = serviceConfig.KeepAliveMaxConnectionIdle()
	s.server.MaxHeaderBytes = rpc.MaxNexusAPIRequestHeaderBytes

	success = true
	return s, nil
}

// Serve serves the Nexus API and does not return until there is a serve error or
// GracefulStop completes. Upon graceful stop, this will return nil. If an error
// is returned, the message is clear that it came from the Nexus API server.
func (h *NexusAPIServer) Serve() error {
	err := h.server.Serve(h.listener)
	// If the error is for close, we have to wait for the shutdown to complete and
	// we don't consider it an error
	if errors.Is(err, http.ErrServerClosed) {
		<-h.stopped
		err = nil
	}
	// Wrap the error to be clearer it's from the Nexus API
	if err != nil {
		return fmt.Errorf("nexus API serve failed: %w", err)
	}
	return nil
}

// GracefulStop stops the Nexus server. This will first attempt a graceful stop
// with a drain time, then will hard-stop. This will not return until stopped.
func (h *NexusAPIServer) GracefulStop(gracefulDrainTime time.Duration) {
	// We try a graceful stop for the amount of time we can drain, then we do a
	// hard stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulDrainTime)
	defer cancel()
	// We intentionally ignore this error, we're gonna stop at this point no
	// matter what. This closes the listener too.
	_ = h.server.Shutdown(shutdownCtx)
	_ = h.server.Close()
	close(h.stopped)
}

func (h *NexusAPIServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: panic handler
	// TODO: metrics
	// Limit the request body to max allowed Payload size. This is hardcoded to 2MB-16KB for headers at the moment.
	r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
	h.logger.Debug(
		"Nexus HTTP API call",
		tag.NewStringTag("http-method", r.Method),
		tag.NewAnyTag("http-url", r.URL),
	)

	// TODO: tidy this up, use the completion handler from the nexus SDK
	if r.URL.Path == "/system/callback" {
		fmt.Println("AAAAAAAAAAAAAAAA Got /system/callback")
		q := r.URL.Query()
		namespaceName := q.Get("namespace")
		// TODO: validations
		workflowID := q.Get("workflow_id")
		runID := q.Get("run_id")
		scheduledEventIDStr := q.Get("scheduled_event_id")
		scheduledEventID, err := strconv.ParseInt(scheduledEventIDStr, 10, 64)
		if err != nil {
			h.logger.Error("nexus callback: invalid scheduled_event_id", tag.WorkflowNamespace(namespaceName), tag.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
		namespace, err := h.namespaceRegistry.GetNamespace(namespace.Name(namespaceName))
		if err != nil {
			h.logger.Error("nexus callback: failed to find namespace", tag.WorkflowNamespace(namespaceName), tag.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			h.logger.Error("nexus callback: failed to read request body", tag.WorkflowNamespace(namespaceName), tag.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, err = h.historyClient.CompleteNexusOperation(r.Context(), &historyservice.CompleteNexusOperationRequest{
			NamespaceId:      namespace.ID().String(),
			WorkflowId:       workflowID,
			RunId:            runID,
			ScheduledEventId: scheduledEventID,
			Payload: &nexuspb.Payload{
				Headers: map[string]*nexuspb.HeaderValues{"Content-Type": {Elements: []string{"application/json"}}},
				Body:    b,
			},
		})
		fmt.Println("AAAAAAAAAAAAAAAA complete request sent to history, error:", err)
		if err != nil {
			h.logger.Error("nexus callback: failed to send completion to history service", tag.WorkflowNamespace(namespaceName), tag.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	if service := h.serviceRegistry.MatchURL(r.URL); service != nil {
		namespace, err := h.namespaceRegistry.GetNamespace(namespace.Name(service.Namespace))
		if err != nil {
			h.logger.Error("failed to get namespace by name", tag.Error(err), tag.WorkflowNamespace(service.Namespace))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// TODO: histogram
		// TODO: escape service name
		r = r.WithContext(context.WithValue(r.Context(), nexusContextKey{}, nexusContext{service: service, namespace: namespace}))
		http.StripPrefix("/"+service.Name, h.nexusHandler).ServeHTTP(w, r)
		return
	}
	failure := nexus.Failure{Message: "not found"}
	body, err := json.Marshal(failure)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	w.Write(body)
}
