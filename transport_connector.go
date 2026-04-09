package sdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/bubustack/bubu-sdk-go/pkg/observability"
	"github.com/bubustack/core/contracts"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultDialTimeout = 10 * time.Second
const (
	defaultReconnectBaseBackoff = 500 * time.Millisecond
	defaultReconnectMaxBackoff  = 30 * time.Second
	defaultReconnectMaxRetries  = 10
)

// TransportConnectorClient wraps the generic Engram↔connector gRPC contract.
// It dials the connector endpoint advertised via the TransportBinding env payload.
type TransportConnectorClient struct {
	conn   *grpc.ClientConn
	client transportpb.TransportConnectorServiceClient
}

// DialTransportConnector establishes a gRPC client connection to the generic transport connector.
// Endpoint may be tcp host:port or a unix domain socket (prefixed with unix://).
func DialTransportConnector(
	ctx context.Context,
	endpoint string,
	opts ...grpc.DialOption,
) (*TransportConnectorClient, error) {
	return dialTransportConnector(ctx, endpoint, newEnvResolver(nil), opts...)
}

func dialTransportConnector(
	ctx context.Context,
	endpoint string,
	env envResolver,
	opts ...grpc.DialOption,
) (*TransportConnectorClient, error) {
	if strings.TrimSpace(endpoint) == "" {
		return nil, fmt.Errorf("transport connector endpoint empty")
	}

	baseDialOpts, err := defaultTransportDialOptions(endpoint, env)
	if err != nil {
		return nil, err
	}
	dialOpts := append(baseDialOpts, opts...)
	if isUnixEndpoint(endpoint) {
		dialOpts = append(dialOpts, grpc.WithContextDialer(unixDialer()))
		endpoint = strings.TrimPrefix(endpoint, "unix://")
	}

	conn, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	waitCtx, cancel := contextWithDialTimeout(ctx, env)
	defer cancel()
	if err := transportconnector.WaitForReady(waitCtx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	if isDebugEnabled() {
		LoggerFromContext(ctx).Debug("Connector dial complete",
			slog.String("endpoint", endpoint),
			slog.Bool("tls", !usesPlaintextLocalConnector(endpoint)),
		)
	}

	return &TransportConnectorClient{
		conn:   conn,
		client: transportpb.NewTransportConnectorServiceClient(conn),
	}, nil
}

// Client exposes the underlying generated gRPC client.
func (c *TransportConnectorClient) Client() transportpb.TransportConnectorServiceClient {
	if c == nil {
		return nil
	}
	return c.client
}

// Close tears down the connector connection.
func (c *TransportConnectorClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func defaultTransportDialOptions(endpoint string, env envResolver) ([]grpc.DialOption, error) {
	if err := validateTransportSecurityMode(env); err != nil {
		return nil, err
	}
	callOpts := transportconnector.ClientCallOptions(
		env,
		transportconnector.DefaultMaxMessageSize,
		transportconnector.DefaultMaxMessageSize,
	)
	dialTimeout := transportconnector.DialTimeout(env, defaultDialTimeout)
	connectParams := grpc.ConnectParams{}
	if dialTimeout > 0 {
		connectParams.MinConnectTimeout = dialTimeout
	}

	creds := defaultTransportCredentials(endpoint)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(connectParams),
	}
	if len(callOpts) > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(callOpts...))
	}
	if observability.TracePropagationEnabled() {
		opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}
	return opts, nil
}

func defaultTransportCredentials(endpoint string) credentials.TransportCredentials {
	if usesPlaintextLocalConnector(endpoint) {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13})
}

func unixDialer() func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		d := &net.Dialer{}
		return d.DialContext(ctx, "unix", addr)
	}
}

func isUnixEndpoint(endpoint string) bool {
	return strings.HasPrefix(endpoint, "unix://")
}

func usesPlaintextLocalConnector(endpoint string) bool {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return false
	}
	if isUnixEndpoint(endpoint) {
		return true
	}
	host := endpoint
	if strings.Contains(endpoint, ":") {
		parsedHost, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			return false
		}
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var connectorDial = func(
	ctx context.Context,
	endpoint string,
	env envResolver,
	opts ...grpc.DialOption,
) (*TransportConnectorClient, error) {
	return dialTransportConnector(ctx, endpoint, env, opts...)
}

func contextWithDialTimeout(ctx context.Context, env envResolver) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	timeout := transportconnector.DialTimeout(env, defaultDialTimeout)
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func validateTransportSecurityMode(env envResolver) error {
	mode := strings.ToLower(strings.TrimSpace(env.lookup(contracts.TransportSecurityModeEnv)))
	if mode == "" || mode == contracts.TransportSecurityModeTLS {
		return nil
	}
	return fmt.Errorf("invalid %s %q: only %q is supported", contracts.TransportSecurityModeEnv, mode,
		contracts.TransportSecurityModeTLS)
}
