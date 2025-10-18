package sdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bubu-sdk-go/pkg/observability"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
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
	client transportpb.TransportConnectorClient
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

	dialOpts := append(defaultTransportDialOptions(env), opts...)
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
	if err := waitForReady(waitCtx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	if isDebugEnabled() {
		LoggerFromContext(ctx).Debug("Connector dial complete",
			slog.String("endpoint", endpoint),
			slog.Bool("insecure", allowInsecureTransport(env)),
		)
	}

	return &TransportConnectorClient{
		conn:   conn,
		client: transportpb.NewTransportConnectorClient(conn),
	}, nil
}

// Client exposes the underlying generated gRPC client.
func (c *TransportConnectorClient) Client() transportpb.TransportConnectorClient {
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

func defaultTransportDialOptions(env envResolver) []grpc.DialOption {
	maxRecv := getEnvInt(DefaultMaxMessageSize, contracts.GRPCClientMaxRecvBytesEnv, env)
	maxSend := getEnvInt(DefaultMaxMessageSize, contracts.GRPCClientMaxSendBytesEnv, env)
	dialTimeout := resolveDialTimeout(env)
	connectParams := grpc.ConnectParams{}
	if dialTimeout > 0 {
		connectParams.MinConnectTimeout = dialTimeout
	}

	var creds credentials.TransportCredentials
	if allowInsecureTransport(env) {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxRecv),
			grpc.MaxCallSendMsgSize(maxSend),
		),
		grpc.WithConnectParams(connectParams),
	}
	if observability.TracePropagationEnabled() {
		opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}
	return opts
}

func allowInsecureTransport(env envResolver) bool {
	mode := strings.ToLower(strings.TrimSpace(env.lookup(contracts.TransportSecurityModeEnv)))
	return mode == contracts.TransportSecurityModePlaintext
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

var connectorDial = func(
	ctx context.Context,
	endpoint string,
	env envResolver,
	opts ...grpc.DialOption,
) (*TransportConnectorClient, error) {
	return dialTransportConnector(ctx, endpoint, env, opts...)
}

func getEnvInt(def int, name string, env envResolver) int {
	if v := env.lookup(name); strings.TrimSpace(v) != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return def
}

func contextWithDialTimeout(ctx context.Context, env envResolver) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	timeout := resolveDialTimeout(env)
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		conn.Connect()
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("connector stuck in %s state", state)
		}
	}
}

func resolveDialTimeout(env envResolver) time.Duration {
	raw := strings.TrimSpace(env.lookup(contracts.GRPCDialTimeoutEnv))
	if raw == "" {
		return defaultDialTimeout
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	return defaultDialTimeout
}
