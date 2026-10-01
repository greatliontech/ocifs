# A filtered, paginating ProjFS enumeration has no dedicated test

The ProjFS provider serves directory enumeration from a
comparator-sorted snapshot with a resumable cursor, a DOS wildcard
search filtering entries as the cursor advances
(`internal/projfsfs/provider_windows.go`, REQ-proj-enumeration). The
windows row witnesses pagination over an unfiltered directory and
wildcard filtering over a directory that fits one buffer; a search
over a directory that pages — the cursor resuming past filtered-out
entries across buffers — is exercised by no test.

Lands: when the windows row witnesses a filtered enumeration that
pages.
