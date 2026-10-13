//go:build !unix

package metrics

import "os/exec"

func configureCommandProcess(*exec.Cmd) {}

func cleanupCommandProcess(*exec.Cmd) {}
