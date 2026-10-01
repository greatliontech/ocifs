# A placeholder held open by a foreign process survives the residue sweep

projection.md's read-only contract has the projected placeholder
state removed at unmount, foreign files and their spine excepted.
The windows backend's residue sweep (`internal/projfsfs/serve_windows.go`)
removes each placeholder, clears a read-only attribute metadata dirt
left and retries once; a placeholder a foreign process holds open
without delete sharing answers the removal with a sharing violation
and survives, a deviation the sweep neither retries later nor
reports. No test on the windows row holds a placeholder open across
an unmount.

Lands: when the windows row probes a placeholder held open by a
foreign process at unmount and the sweep's answer — a later retry, a
report entry, or the spec stating the leftover — is dispositioned.
