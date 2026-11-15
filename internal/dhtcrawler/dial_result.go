package dhtcrawler

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
)

func classifyDialError(err error) string {
	if err == nil {
		return "ok"
	}

	if errors.Is(err, metainforequester.ErrLimited) {
		return "limited"
	}

	if errors.Is(err, metainforequester.ErrSelfPeer) {
		return "self"
	}

	if errors.Is(err, metainforequester.ErrClosedNetworkPeer) {
		return "closed_network_peer"
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		switch {
		case errors.Is(err, syscall.ECONNREFUSED):
			return "dial_refused"
		case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
			return "dial_unreachable"
		case opErr.Timeout(), errors.Is(err, context.DeadlineExceeded):
			return "dial_timeout"
		default:
			return "dial_other"
		}
	}

	msg := err.Error()

	if strings.HasPrefix(msg, "utp dial: ") {
		return "utp_dial_failed"
	}

	if strings.HasPrefix(msg, "mse retry: ") {
		inner := errors.Unwrap(err)
		if strings.HasPrefix(inner.Error(), "mse handshake: ") {
			return "mse_handshake_" + classifyReadError(inner)
		}

		return "mse_" + classifyDialError(inner)
	}

	switch {
	case strings.Contains(msg, "failed to read all handshake bytes"):
		return "bt_handshake_" + classifyReadError(err)
	case strings.Contains(msg, "invalid handshake response"):
		return "bt_handshake_invalid"
	case strings.Contains(msg, "does not support the extension protocol"):
		return "no_extension_protocol"
	case strings.Contains(msg, "infohash mismatch"):
		return "infohash_mismatch"
	case strings.Contains(msg, "first extension message"),
		strings.Contains(msg, "ut_metadata is not"):
		return "ex_handshake_invalid"
	case strings.Contains(msg, "metadata too big"):
		return "no_metadata_size"
	case strings.Contains(msg, "rejected sending"):
		return "rejected"
	case strings.Contains(msg, "metadataPiece"), strings.Contains(msg, "receivedSize"),
		strings.Contains(msg, "longer than max allowed"):
		return "piece_invalid"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}

	if r := classifyReadError(err); r != "other" {
		return "after_connect_" + r
	}

	return "other"
}

func classifyReadError(err error) string {
	var netErr net.Error

	switch {
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return "reset"
	default:
		return "other"
	}
}
