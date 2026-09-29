# vpsmonlib

Shared Linux metric collection for [vpsmon](https://github.com/leodeim/vpsmon) and [vpsagent](https://github.com/leodeim/vpsagent).

This module reads local system data. It has no HTTP client, Cloud endpoint, credentials, or telemetry uploader. `vpsmon` enables the detailed dashboard collection; `vpsagent` selects only the data needed for Cloud telemetry.
