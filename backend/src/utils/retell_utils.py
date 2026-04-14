"""
Retell AI: inbound webhooks, signature verification, and call_history updates.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Disconnection reasons that mean the callee never connected (inbound: caller abandoned / line issues)
_UNANSWERED_REASONS = frozenset(
    {
        "dial_failed",
        "dial_no_answer",
        "dial_busy",
        "invalid_destination",
        "telephony_provider_permission_denied",
        "sip_routing_error",
        "marked_as_spam",
        "user_declined",
    }
)


def ms_epoch_to_datetime(ms: Any) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        v = int(ms)
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def disconnection_reason_to_final_status(reason: Optional[str]) -> str:
    if not reason:
        return "completed"
    r = str(reason).lower()
    if r in _UNANSWERED_REASONS or r.startswith("dial_"):
        return "unanswered"
    return "completed"


def verify_retell_signature(raw_body: str, signature: Optional[str]) -> bool:
    """
    Official Retell verification: raw request body + X-Retell-Signature + RETELL_API_KEY
    (retell-sdk Retell.verify). This differs from a simple HMAC(body, RETELL_WEBHOOK_SECRET)
    pattern; use the API key labeled for webhooks in the Retell dashboard.
    """
    if os.getenv("VERIFY_RETELL_WEBHOOK", "true").lower() in ("0", "false", "no"):
        return True
    api_key = os.getenv("RETELL_API_KEY")
    if not api_key:
        logger.warning("RETELL_API_KEY not set; rejecting Retell webhook")
        return False
    if not signature:
        return False
    try:
        from retell import Retell

        client = Retell(api_key=api_key)
        return bool(
            client.verify(
                raw_body,
                api_key=str(api_key),
                signature=str(signature),
            )
        )
    except Exception as e:
        logger.error("Retell signature verify failed: %s", e)
        return False


def _load_inbound_number_user_map() -> dict[str, int]:
    raw = os.getenv("RETELL_INBOUND_NUMBER_USER_MAP", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        out: dict[str, int] = {}
        for k, v in data.items():
            out[str(k)] = int(v)
        return out
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.warning("Invalid RETELL_INBOUND_NUMBER_USER_MAP: %s", e)
        return {}


def default_retell_user_id_from_env() -> Optional[int]:
    """When Retell omits metadata/numbers or mapping fails, use this users.id (single-tenant)."""
    raw = os.getenv("RETELL_DEFAULT_INBOUND_USER_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid RETELL_DEFAULT_INBOUND_USER_ID: %r", raw)
        return None


def resolve_inbound_user_id(to_number: Optional[str]) -> Optional[int]:
    """Map Retell inbound callee number (your number) to a user_id."""
    if not to_number:
        return default_retell_user_id_from_env()
    m = _load_inbound_number_user_map()
    if to_number in m:
        return m[to_number]
    compact = "".join(ch for ch in to_number if ch.isdigit() or ch == "+")
    if compact != to_number and compact in m:
        return m[compact]
    return default_retell_user_id_from_env()


def resolve_user_from_call(call: dict[str, Any]) -> Optional[int]:
    meta = call.get("metadata")
    if isinstance(meta, dict) and meta.get("user_id") is not None:
        try:
            return int(meta["user_id"])
        except (TypeError, ValueError):
            pass
    uid = resolve_inbound_user_id(call.get("to_number"))
    if uid is not None:
        return uid
    uid = resolve_inbound_user_id(call.get("from_number"))
    if uid is not None:
        return uid
    return default_retell_user_id_from_env()


def resolve_agent_tool_user_id(raw: Any) -> Optional[int]:
    """
    Prefer tool JSON user_id; fall back to RETELL_DEFAULT_INBOUND_USER_ID.
    Treats 0 / negative / empty-string / None as "not provided" — Retell injects
    the literal integer 0 when {{user_id}} has no real value (e.g. test calls from
    the Retell dashboard where the inbound webhook hasn't fired).
    """
    s = None
    try:
        s = str(raw).strip() if raw is not None else None
    except Exception:
        s = None
    # Retell sometimes passes literal template strings like "{{user_id}}" when unset.
    if s and ("{{" in s or "}}" in s):
        s = ""

    if s is not None and s.lower() not in ("", "0", "null", "none"):
        try:
            v = int(s)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    return default_retell_user_id_from_env()


def build_transcript_payload(call: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Shape transcript for JSONB storage (matches mixed formats fetch_and_store_transcript accepts)."""
    tw = call.get("transcript_with_tool_calls")
    tobj = call.get("transcript_object")
    tstr = call.get("transcript")
    if tw is not None:
        return {"source": "retell", "transcript_with_tool_calls": tw}
    if tobj is not None:
        return {"source": "retell", "transcript_object": tobj}
    if tstr is not None:
        return {"source": "retell", "transcript_text": tstr}
    return None


def extract_call_summary(call: dict[str, Any]) -> Optional[str]:
    ca = call.get("call_analysis")
    if not isinstance(ca, dict):
        return None
    return ca.get("call_summary") or ca.get("summary")


def format_call_analysis_for_db(call: dict[str, Any]) -> Optional[str]:
    """
    Match reference project behavior: persist summary, sentiment, and custom_analysis_data
    into call_history.summary (TEXT).
    """
    ca = call.get("call_analysis")
    if not isinstance(ca, dict):
        return None
    parts: list[str] = []
    cs = ca.get("call_summary") or ca.get("summary")
    if cs:
        parts.append(str(cs).strip())
    sent = ca.get("user_sentiment")
    if sent:
        parts.append(f"Sentiment: {sent}")
    custom = ca.get("custom_analysis_data")
    if isinstance(custom, dict) and custom:
        try:
            custom_s = json.dumps(custom, ensure_ascii=False, default=str)
            if len(custom_s) > 4000:
                custom_s = custom_s[:4000] + "…"
            parts.append(f"Custom analysis: {custom_s}")
        except Exception:
            pass
    if not parts:
        return None
    return "\n\n".join(parts)


def _event_dedupe_key(event: Optional[str], call_id: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"{event or 'unknown'}:{call_id}:{h}"


def extract_retell_event_and_call(payload: dict[str, Any]) -> tuple[Optional[str], dict[str, Any]]:
    """
    Normalize Retell POST body: event name + call-shaped dict.
    Some deliveries nest fields under 'call'; others duplicate keys at the root — merge both.
    """
    if not isinstance(payload, dict):
        return None, {}
    ev = payload.get("event") or payload.get("event_type") or payload.get("type")
    if isinstance(ev, str):
        ev = ev.strip().lower() or None
    raw_call = payload.get("call")
    call: dict[str, Any] = dict(raw_call) if isinstance(raw_call, dict) else {}
    spill_keys = (
        "call_id",
        "agent_id",
        "call_type",
        "from_number",
        "to_number",
        "start_timestamp",
        "end_timestamp",
        "duration_ms",
        "disconnection_reason",
        "transcript",
        "transcript_object",
        "transcript_with_tool_calls",
        "recording_url",
        "recording_multi_channel_url",
        "call_analysis",
        "metadata",
        "collected_dynamic_variables",
    )
    for k in spill_keys:
        if k in payload and payload[k] is not None and (k not in call or call[k] is None):
            call[k] = payload[k]
    cid = call.get("call_id")
    if cid is not None:
        call["call_id"] = str(cid)
    return ev, call

def _norm_event(ev: Optional[str]) -> Optional[str]:
    if not ev:
        return None
    e = str(ev).strip().lower()
    if not e:
        return None
    # tolerate common variations / older names
    aliases = {
        "call_end": "call_ended",
        "ended": "call_ended",
        "call_start": "call_started",
        "started": "call_started",
        "analyzed": "call_analyzed",
        "analysis_completed": "call_analyzed",
        "transcript": "transcript_updated",
    }
    return aliases.get(e, e)


def _should_apply_status(db, call_id: str, new_status: str) -> bool:
    if new_status not in {"completed", "unanswered", "connected", "dialing", "initialized", "initiated"}:
        return True
    conn = db.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM call_history WHERE call_id = %s",
                (call_id,),
            )
            row = cursor.fetchone()
            if not row:
                return True
            cur = row[0]
            if cur in ("completed", "unanswered"):
                return new_status == cur
            return True
    finally:
        db.release_connection(conn)


def apply_retell_webhook_event(db, payload: dict[str, Any]) -> None:
    """Normalize Retell POST /retell-webhook JSON into call_history."""
    from src.utils.utils import calculate_duration

    event, call = extract_retell_event_and_call(payload)
    event = _norm_event(event)
    call_id = call.get("call_id")
    if not call_id:
        logger.warning(
            "Retell webhook missing call_id (check root vs call object). keys=%s",
            list(payload.keys())[:20] if isinstance(payload, dict) else None,
        )
        return

    logger.info("Retell webhook received event=%s call_id=%s", event, call_id)

    # If transcript/analyzed arrives before started, create a minimal row so we can persist fields.
    # (Retell delivery order can vary.)
    def _ensure_row_exists(default_status: str = "connected") -> None:
        if db.call_exists(call_id):
            return
        user_id = resolve_user_from_call(call)
        if user_id is None:
            logger.error("Cannot create call_history row without user_id for call_id=%s", call_id)
            return
        db.insert_call_history(
            user_id=user_id,
            call_id=call_id,
            status=default_status,
            voice_name="retell",
            to_number=call.get("to_number"),
            from_number=call.get("from_number"),
        )

    if event in (
        "transfer_started",
        "transfer_bridged",
        "transfer_cancelled",
        "transfer_ended",
    ):
        dedupe_t = _event_dedupe_key(event, call_id, payload)
        if db.retell_webhook_event_seen(call_id, dedupe_t):
            return
        if db.call_exists(call_id):
            db.append_events_log_entry(call_id, f"retell_{event}", payload)
            try:
                db.add_agent_event(call_id, f"retell_{event}", {"payload": payload})
            except Exception:
                pass
        db.record_retell_webhook_event(call_id, dedupe_t, event)
        return

    dedupe = _event_dedupe_key(event, call_id, payload)
    if db.retell_webhook_event_seen(call_id, dedupe):
        logger.info("Skipping duplicate Retell event %s for %s", event, call_id)
        return

    if event == "call_started":
        user_id = resolve_user_from_call(call)
        if user_id is None:
            logger.error("No user_id resolved for Retell call_started %s", call_id)
            return
        started_at = ms_epoch_to_datetime(call.get("start_timestamp"))
        updates = {
            "status": "connected",
            "from_number": call.get("from_number"),
            "to_number": call.get("to_number"),
        }
        if started_at:
            updates["started_at"] = started_at
        if not db.call_exists(call_id):
            db.insert_call_history(
                user_id=user_id,
                call_id=call_id,
                status="connected",
                voice_name="retell",
                to_number=call.get("to_number"),
                from_number=call.get("from_number"),
            )
            db.update_call_history(call_id, {k: v for k, v in updates.items() if v is not None})
        else:
            if _should_apply_status(db, call_id, "connected"):
                db.update_call_history(call_id, {k: v for k, v in updates.items() if v is not None})
        db.append_events_log_entry(call_id, "retell_call_started", payload)
        try:
            db.add_agent_event(call_id, "retell_call_started", {"call": call})
        except Exception:
            pass
        db.record_retell_webhook_event(call_id, dedupe, event)
        return

    if event == "transcript_updated":
        if not db.call_exists(call_id):
            _ensure_row_exists(default_status="connected")
        if not db.call_exists(call_id):
            # still can't create row (likely missing user_id)
            logger.info("Retell transcript_updated ignored: no call_history row for call_id=%s", call_id)
            return
        snap = build_transcript_payload(call)
        if snap:
            db.update_call_history(call_id, {"transcript": snap})
        db.append_events_log_entry(call_id, "retell_transcript_updated", payload)
        db.record_retell_webhook_event(call_id, dedupe, event)
        return

    if event == "call_ended":
        user_id = resolve_user_from_call(call)
        reason = call.get("disconnection_reason")
        final_status = disconnection_reason_to_final_status(reason)
        ended_at = ms_epoch_to_datetime(call.get("end_timestamp"))
        started_at = ms_epoch_to_datetime(call.get("start_timestamp"))
        duration_ms = call.get("duration_ms")
        if duration_ms is not None:
            try:
                duration_sec = float(duration_ms) / 1000.0
            except (TypeError, ValueError):
                duration_sec = calculate_duration(started_at, ended_at)
        else:
            duration_sec = calculate_duration(started_at, ended_at)

        transcript = build_transcript_payload(call)
        recording = call.get("recording_url") or call.get("recording_multi_channel_url")

        updates: dict[str, Any] = {
            "status": final_status,
            "from_number": call.get("from_number"),
            "to_number": call.get("to_number"),
        }
        if ended_at:
            updates["ended_at"] = ended_at
        if started_at:
            updates["started_at"] = started_at
        if duration_sec is not None:
            updates["duration"] = max(0.0, float(duration_sec))
        if transcript:
            updates["transcript"] = transcript
        if recording:
            updates["recording_url"] = recording

        if not db.call_exists(call_id):
            if user_id is None:
                logger.error("call_ended: cannot insert row without user_id for %s", call_id)
                return
            db.insert_call_history(
                user_id=user_id,
                call_id=call_id,
                status=final_status,
                voice_name="retell",
                to_number=call.get("to_number"),
                from_number=call.get("from_number"),
            )
            db.update_call_history(call_id, {k: v for k, v in updates.items() if v is not None})
        else:
            safe: dict[str, Any] = {}
            for k, v in updates.items():
                if k == "status" and not _should_apply_status(db, call_id, str(v)):
                    continue
                safe[k] = v
            db.update_call_history(call_id, {k: v for k, v in safe.items() if v is not None})

        db.append_events_log_entry(call_id, "retell_call_ended", payload)
        try:
            db.add_agent_event(call_id, "retell_call_ended", {"disconnection_reason": reason, "call": call})
        except Exception:
            pass
        # Post-call extracted fields (same idea as reference project's collected_dynamic_variables)
        collected = call.get("collected_dynamic_variables")
        if collected and isinstance(collected, dict):
            try:
                db.add_agent_event(
                    call_id,
                    "retell_collected_dynamic_variables",
                    {"data": collected},
                )
            except Exception:
                pass
        db.record_retell_webhook_event(call_id, dedupe, event)
        return

    if event == "call_analyzed":
        if not db.call_exists(call_id):
            _ensure_row_exists(default_status="connected")
        if not db.call_exists(call_id):
            return
        summary = format_call_analysis_for_db(call) or extract_call_summary(call)
        if summary:
            db.update_call_history(call_id, {"summary": summary})
        db.append_events_log_entry(call_id, "retell_call_analyzed", payload)
        try:
            db.add_agent_event(call_id, "retell_call_analyzed", {"call_analysis": call.get("call_analysis")})
        except Exception:
            pass
        db.record_retell_webhook_event(call_id, dedupe, event)
        return

    _handled = {
        "call_started",
        "transcript_updated",
        "call_ended",
        "call_analyzed",
        "transfer_started",
        "transfer_bridged",
        "transfer_cancelled",
        "transfer_ended",
    }
    if event not in _handled:
        logger.warning("Retell webhook unhandled event=%r call_id=%s", event, call_id)

    db.record_retell_webhook_event(call_id, dedupe, event)


def retell_get_call(call_id: str) -> dict[str, Any]:
    """Fetch current call state from Retell HTTP API (dashboard / live view)."""
    import requests

    api_key = (os.getenv("RETELL_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("RETELL_API_KEY is not configured")
    url = f"https://api.retellai.com/v2/get-call/{call_id}"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=45,
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected Retell get-call response")
    return data
