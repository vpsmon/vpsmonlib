# vpsmonlib

Shared Linux metric collection for [vpsmon](https://github.com/leodeim/vpsmon) and [vpsagent](https://github.com/vpsmon/vpsagent).

This module reads local system data. It has no HTTP client, Cloud endpoint, credentials, or telemetry uploader. `vpsmon` enables the detailed dashboard collection; `vpsagent` selects only the data needed for Cloud telemetry.

Disk usage probes use two scheduler workers and a fixed budget of 32 outstanding
per-mount calls. Linux `statfs` calls blocked inside the kernel cannot be
canceled; after 100 ms the scheduler moves on while the call retains its slot
until it really returns. This lets healthy mounts continue after stalled calls
without creating unbounded goroutines. Thirty-two distinct permanent stalls
eventually use the full budget, so newer mounts can be omitted until a call
returns. Disk collection reports `complete`, `partial`, or `unavailable` in
`disk_collection_status`; failed probes are omitted rather than reusing old
values as fresh readings.

If no recognized capacity mounts are found, collection is `unavailable`; an
empty disk list does not prove that previously monitored capacity is healthy.
