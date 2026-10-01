package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/greatliontech/gmdb/oslock"
)

// retired reads a retirement's error: nil and the bare deferral are
// a retirement done; a deferral joined with another error keeps the
// other; any other error stands.
func TestRetiredReading(t *testing.T) {
	closeErr := errors.New("close: boom")
	for _, c := range []struct {
		in   error
		want error
	}{
		{nil, nil},
		{oslock.ErrUnlinkDeferred, nil},
		{errors.Join(fmt.Errorf("%w: busy", oslock.ErrUnlinkDeferred)), nil},
		{errors.Join(fmt.Errorf("%w: busy", oslock.ErrUnlinkDeferred), closeErr), closeErr},
		// Retire's own shape: the deferral wrapping the refusal's
		// cause with two %w, joined with a nil close, and bare.
		{errors.Join(fmt.Errorf("%w: %w", oslock.ErrUnlinkDeferred, errors.New("sharing violation")), nil), nil},
		{fmt.Errorf("%w: %w", oslock.ErrUnlinkDeferred, errors.New("sharing violation")), nil},
		{errors.Join(fmt.Errorf("%w: %w", oslock.ErrUnlinkDeferred, errors.New("sharing violation")), closeErr), closeErr},
		{closeErr, closeErr},
	} {
		got := retired(c.in)
		if (c.want == nil) != (got == nil) || (c.want != nil && !errors.Is(got, c.want)) || (got != nil && errors.Is(got, oslock.ErrUnlinkDeferred)) {
			t.Errorf("retired(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
