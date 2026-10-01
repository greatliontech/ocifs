# The layer suite's oracle and three property tests run on linux alone

`internal/layer`'s extraction oracle (`oracle_test.go`) builds its
FIFO cases with mkfifo, and the property tests share its file
(`property_test.go`), so both files carry `//go:build linux`: the
darwin and windows rows run the unification suite (`unify_test.go`,
untagged) but neither the oracle nor the four property tests, three
of which (marker-order independence, the single-layer round trip,
escape rejection) need no FIFO at all and are gated by their file
alone.

Lands: when the three oracle-free property tests run on every row
(their own file, or the oracle's FIFO cases gated alone) and the
oracle builds its FIFO cases without mkfifo or states the cap.
