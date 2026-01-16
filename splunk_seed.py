"""
Upload CSV-exported Splunk events into Splunk via HEC (HTTP Event Collector).

This script is designed for CSV rows like:
  "_serial","_time",source,sourcetype,host,index,"splunk_server","_raw"

It will:
- Parse the CSV with headers
- Parse the `_raw` column as JSON (handling doubled quotes from CSV exports)
- Send each row to HEC with proper metadata (index/source/sourcetype/host)
- Set HEC `time` from the CSV `_time` so Splunk sets `_time` correctly

Usage:
  python splunk_seed.py --csv logs.csv --hec-url http://localhost:8088/services/collector/event --token $SPLUNK_HEC_TOKEN

Env vars (optional defaults):
  SPLUNK_HEC_URL, SPLUNK_HEC_TOKEN, SPLUNK_HEC_VERIFY, SPLUNK_HEC_BATCH_SIZE
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import uuid
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests


def _parse_splunk_csv_time_to_epoch(value: Optional[str]) -> Optional[float]:
    """
    Parse CSV `_time` like `2026-01-09T21:13:43.475+0000` to epoch seconds.
    If parsing fails, return None and let Splunk assign ingestion time.
    """
    if not value:
        return None
    v = value.strip().strip('"')
    try:
        # Example: 2026-01-09T21:13:43.475+0000
        dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f%z")
        return dt.timestamp()
    except ValueError:
        pass
    try:
        # Fallback: ISO-ish without millis
        dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S%z")
        return dt.timestamp()
    except ValueError:
        return None


def _parse_raw_json(raw_value: str) -> Dict[str, Any]:
    """
    Parse `_raw` JSON. Splunk CSV exports often double quotes inside a quoted field.
    """
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return {"_raw": "", "parse_error": True}

    # Typical CSV export encoding: {""k"":""v""} -> {"k":"v"}
    candidate = raw_value.replace('""', '"')
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
        return {"_raw": candidate, "parsed_type": type(parsed).__name__}
    except json.JSONDecodeError:
        # Fallback: store as plain string for visibility
        return {"_raw": raw_value, "parse_error": True}


def _hec_event_from_row(
    row: Dict[str, str],
    default_index: Optional[str] = None,
    force_index: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a Splunk HEC event payload from a CSV row.
    See: https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector
    """
    index = (force_index or row.get("index") or default_index or "").strip() or None
    source = (row.get("source") or "").strip() or None
    sourcetype = (row.get("sourcetype") or "").strip() or None
    host = (row.get("host") or "").strip() or None

    # Prefer CSV `_time` for Splunk event time.
    # Note: this sets Splunk's `_time` (internal), independent of any `time` field inside JSON.
    epoch_time = _parse_splunk_csv_time_to_epoch(row.get("_time"))

    # Event body: parsed JSON from `_raw` if possible; otherwise keep `_raw`.
    event = _parse_raw_json(row.get("_raw", ""))

    # Keep helpful provenance fields (optional, searchable)
    event.setdefault("_csv_source", source)
    event.setdefault("_csv_sourcetype", sourcetype)
    event.setdefault("_csv_host", host)

    payload: Dict[str, Any] = {"event": event}
    if epoch_time is not None:
        payload["time"] = epoch_time
    if index:
        payload["index"] = index
    if source:
        payload["source"] = source
    if sourcetype:
        payload["sourcetype"] = sourcetype
    if host:
        payload["host"] = host

    return payload


def _post_hec_batch(
    session: requests.Session,
    hec_url: str,
    token: str,
    events: List[Dict[str, Any]],
    verify: bool,
    timeout_s: float,
    channel: Optional[str],
) -> None:
    """
    Post a batch using newline-delimited JSON events (HEC supports NDJSON).
    """
    if not events:
        return

    headers = {
        "Authorization": f"Splunk {token}",
        "Content-Type": "application/json",
    }
    # Some Splunk HEC setups (notably when indexer acknowledgements are enabled) require a request channel.
    # See error: {"text":"Data channel is missing","code":10}
    if channel:
        headers["X-Splunk-Request-Channel"] = channel
    data = "\n".join(json.dumps(e, separators=(",", ":")) for e in events)
    resp = session.post(hec_url, headers=headers, data=data, timeout=timeout_s, verify=verify)
    if resp.status_code >= 400:
        # Splunk HEC returns a JSON body explaining the error (e.g., token disabled, incorrect index, HEC disabled).
        body = (resp.text or "").strip()
        snippet = body[:2000]  # keep errors readable
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} from HEC. Response body:\n{snippet}",
            response=resp,
        )


def upload_csv(
    csv_path: str,
    hec_url: str,
    token: str,
    *,
    verify: bool = True,
    batch_size: int = 200,
    timeout_s: float = 10.0,
    default_index: Optional[str] = None,
    force_index: Optional[str] = None,
    channel: Optional[str] = None,
) -> None:
    session = requests.Session()
    batch: List[Dict[str, Any]] = []
    sent = 0

    # CSV exports can have very large `_raw` fields; raise the parser limit.
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        # Fallback for platforms that don't accept sys.maxsize
        csv.field_size_limit(1024 * 1024 * 50)  # 50MB

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            payload = _hec_event_from_row(row, default_index=default_index, force_index=force_index)
            batch.append(payload)
            if len(batch) >= batch_size:
                _post_hec_batch(session, hec_url, token, batch, verify=verify, timeout_s=timeout_s, channel=channel)
                sent += len(batch)
                batch.clear()

        if batch:
            _post_hec_batch(session, hec_url, token, batch, verify=verify, timeout_s=timeout_s, channel=channel)
            sent += len(batch)

    print(f"Uploaded {sent} events to HEC: {hec_url}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload Splunk CSV logs into Splunk HEC")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument(
        "--hec-url",
        default=os.getenv("SPLUNK_HEC_URL", "http://localhost:8088/services/collector/event"),
        help="HEC endpoint URL (default: env SPLUNK_HEC_URL or http://localhost:8088/services/collector/event)",
    )
    parser.add_argument("--token", default=os.getenv("SPLUNK_HEC_TOKEN"), help="HEC token (or env SPLUNK_HEC_TOKEN)")
    parser.add_argument("--default-index", default=os.getenv("SPLUNK_HEC_INDEX"), help="Fallback index if CSV row has no index")
    parser.add_argument(
        "--force-index",
        default=os.getenv("SPLUNK_HEC_FORCE_INDEX"),
        help="Override ALL events to use this index (useful if your token only allows specific indexes)",
    )
    parser.add_argument(
        "--channel",
        default=os.getenv("SPLUNK_HEC_CHANNEL"),
        help="HEC request channel (required when indexer acknowledgements are enabled). Default: random UUID per run.",
    )
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("SPLUNK_HEC_BATCH_SIZE", "200")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("SPLUNK_HEC_TIMEOUT_S", "10.0")))
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Disable TLS verification (useful for self-signed certs)",
    )
    args = parser.parse_args()

    if not args.token:
        raise SystemExit("Missing HEC token. Pass --token or set SPLUNK_HEC_TOKEN.")

    channel = args.channel or str(uuid.uuid4())

    upload_csv(
        csv_path=args.csv,
        hec_url=args.hec_url,
        token=args.token,
        verify=not args.no_verify,
        batch_size=args.batch_size,
        timeout_s=args.timeout,
        default_index=args.default_index,
        force_index=args.force_index,
        channel=channel,
    )


if __name__ == "__main__":
    main()
