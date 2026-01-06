//go:build linux

package server

import "golang.org/x/sys/unix"

// pollRDHUP is POLLRDHUP on Linux, which detects when the remote end closes.
const pollRDHUP = unix.POLLRDHUP
