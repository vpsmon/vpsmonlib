package metrics

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"time"
)

const detailTimeout = 3 * time.Second
const commandOutputLimit = 1 << 20

var errCommandOutputLimit = errors.New("collector command output exceeds limit")

type limitedCommandBuffer struct {
	buffer bytes.Buffer
	cancel context.CancelFunc
	err    error
}

func (b *limitedCommandBuffer) Write(p []byte) (int, error) {
	if len(p) > commandOutputLimit-b.buffer.Len() {
		b.err = errCommandOutputLimit
		b.cancel()
		return 0, errCommandOutputLimit
	}
	return b.buffer.Write(p)
}

// A shared caller deadline bounds a whole optional collection, while the local
// deadline also protects standalone process and log requests.
func commandOutput(parent context.Context, combined bool, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, detailTimeout)
	defer cancel()
	out := &limitedCommandBuffer{cancel: cancel}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = 200 * time.Millisecond
	cmd.Stdout = out
	cmd.Stderr = io.Discard
	if combined {
		cmd.Stderr = out
	}
	if err := cmd.Run(); err != nil {
		return nil, errors.Join(err, out.err, ctx.Err())
	}
	return out.buffer.Bytes(), nil
}
