//go:build !linux

package server

// pollRDHUP is 0 on non-Linux platforms since POLLRDHUP is Linux-specific.
// On these platforms, POLLHUP alone should be sufficient.
const pollRDHUP = 0
