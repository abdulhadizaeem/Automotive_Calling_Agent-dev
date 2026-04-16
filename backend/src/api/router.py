import json
import logging
import os
import io

import traceback
from datetime import datetime, timedelta,timezone
from typing import Dict, List, Optional, Tuple, Any
import requests
import asyncio
from dotenv import load_dotenv
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
)
from datetime import datetime

from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse,StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import HTTPException, Response
from rich import print
from pydantic import BaseModel
from src.api.base_models import (
    UserLogin,
    UserRegister,
    UserOut,
    LoginResponse,
    UpdateUserProfileRequest,
    Assistant_Payload
)
from src.models.System_Prompt import SystemPromptBuilder
from src.utils.db import PGDB 
from src.utils.mail_management import Send_Mail
from src.utils.jwt_utils import create_access_token
from src.utils.utils import get_current_user, calculate_duration
from src.utils.retell_utils import (
    apply_retell_webhook_event,
    resolve_agent_tool_user_id,
    resolve_inbound_user_id,
    retell_get_call,
    retell_get_agent,
    retell_update_agent,
    retell_list_voices,
    retell_get_conversation_flow,
    retell_update_conversation_flow,
)

load_dotenv()

router = APIRouter()
mail_obj = Send_Mail()
db = PGDB()
load_dotenv(override=True)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GCS_BUCKET_NAME = os.getenv("GOOGLE_BUCKET_NAME")
GCS_SERVICE_ACCOUNT_KEY = os.getenv("GCS_SERVICE_ACCOUNT_KEY")  


# error response 
def error_response(message, status_code=400):
    return JSONResponse(
        status_code=status_code,
        content={"error": message}
    )


@router.post("/register")
def register_user(user: UserRegister):
    user_dict = user.dict()
    # 🔽 Normalize both email and username
    user_dict["email"] = user_dict["email"].strip().lower()
    user_dict["username"] = user_dict["username"].strip().lower()
    user_dict['is_admin'] = True
    try:
        db.register_user(user_dict)
        return JSONResponse(status_code=201, content={"message": "You are registered successfully."})
    except ValueError as ve:
        return error_response(status_code=400, message=str(ve))
    except Exception as e:
        traceback.print_exc()
        return error_response(status_code=500, message=f"Registration failed: {str(e)}")

@router.post("/login",response_model=LoginResponse,)
def login_user(user: UserLogin):
    try:
        user_dict = {
        "email": user.email,
        "password": user.password
    }
        logging.info(f"User dict: {user_dict}")
        user_dict["email"] = user_dict["email"].strip().lower()
        result = db.login_user(user_dict)
        if not result:
            return error_response("Invalid username or password", status_code=422)
        
        
        token = create_access_token({"sub": str(result["id"])})
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": result
        }
        
    except ValueError as ve:
        # Return 401 when credentials are invalid
        return error_response(str(ve),status_code=422)

    except Exception as e:
        logging.error(f"Error during login: {str(e)}")
        return error_response(f"Internal server error: {str(e)}",status_code=500)
    


voices = {
    # English voices
    "david": "1SM7GgM6IMuvQlz2BwM3",
    "ravi": "A7AUsa1uITCDpK29MG3m",
    "emily-british": "YWmufCrZ2agGoSoVL8je",
    "alice-british": "bMxLr8fP6hzNRRi9nJxU",
    "julia-british": "ZtcPZrt9K4w8e1OB9M6w",
    
    # Spanish voicesYWmufCrZ2agGoSoVL8je
    "julio": "A7AUsa1uITCDpK29MG3m",
    "donato": "851ejYcv2BoNPjrkw93G",
    "helena-spanish": "5vkxOzoz40FrElmLP4P7",
    "rosa": "BIvP0GN1cAtSRTxNHnWS",
    "mariam": "90ipbRoKi4CpHXvKVtl0",
}

@router.post("/assistant-initiate-call")
async def assistant_initiate_call(payload: Assistant_Payload, user=Depends(get_current_user)):
    """
    Inbound mode: does not place an outbound dial. Returns the Retell number customers call;
    call rows are created when Retell sends webhooks (call_started / call_ended).
    """
    try:
        voice_name = getattr(payload, "voice", "david").lower()
        voice_id = voices.get(voice_name)
        if not voice_id:
            logging.warning("Unknown voice '%s', using default 'david'", voice_name)
            voice_name = "david"

        language = getattr(payload, "language", "en").lower()
        valid_languages = ["en", "es", "german", "italian", "french"]
        if language not in valid_languages:
            logging.warning("Unknown language '%s', defaulting to 'en'", language)
            language = "en"

        user_prompt_data = db.get_user_prompt(user["id"])
        if not user_prompt_data:
            return error_response("User prompt not found", status_code=404)

        base_prompt = user_prompt_data["system_prompt"]
        prompt_builder = SystemPromptBuilder(
            base_prompt=base_prompt,
            caller_name=payload.caller_name,
            caller_email=payload.caller_email,
            call_context=payload.context,
            language=language,
        )
        complete_system_prompt = prompt_builder.generate_complete_prompt()
        logging.info("Prepared system prompt (%s chars) for user %s", len(complete_system_prompt), user["id"])

        # Store inbound settings so Retell inbound webhook can populate dynamic variables used by your Conversation Flow.
        business_name = (payload.caller_name or "").strip() or None
        agent_name = "SUMA"
        try:
            db.upsert_inbound_call_settings(
                user_id=user["id"],
                business_name=business_name,
                call_context=(payload.context or "").strip() or None,
                agent_name=agent_name,
            )
        except Exception as e:
            logging.warning("Failed to store inbound_call_settings for user %s: %s", user["id"], e)

        inbound_number = (
            os.getenv("RETELL_INBOUND_NUMBER")
            or os.getenv("RETELL_PUBLIC_INBOUND_NUMBER")
            or ""
        )

        return JSONResponse(
            {
                "success": True,
                "direction": "inbound",
                "call_id": None,
                "dispatch_id": None,
                "inbound_number": inbound_number,
                "voice": voice_name,
                "language": language,
                "message": "Inbound mode: customers call your Retell number; calls are logged when webhooks fire. Configure RETELL_INBOUND_NUMBER and map users via RETELL_INBOUND_NUMBER_USER_MAP or RETELL_DEFAULT_INBOUND_USER_ID.",
            }
        )

    except Exception as e:
        logging.error("assistant-initiate-call error: %s", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to prepare inbound call: {str(e)}")


@router.post("/retell-webhook")
async def retell_webhook(request: Request):
    """Retell account/agent webhook: call_started, transcript_updated, call_ended, call_analyzed, transfers."""
    raw_body = (await request.body()).decode("utf-8")
    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"message": "Invalid JSON"})
    try:
        apply_retell_webhook_event(db, data)
    except Exception as e:
        logging.error("retell-webhook handler error: %s", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    return Response(status_code=204)


@router.post("/retell-inbound-webhook")
async def retell_inbound_webhook(request: Request):
    """
    Retell inbound routing webhook (event=call_inbound).
    Configure this URL in Retell for your inbound number.
    """
    raw_body = (await request.body()).decode("utf-8")
    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"message": "Invalid JSON"})
    if data.get("event") != "call_inbound":
        return JSONResponse(status_code=200, content={})

    ci = data.get("call_inbound") or {}
    to_number = ci.get("to_number")
    user_id = resolve_inbound_user_id(to_number)

    body: dict = {"call_inbound": {}}
    agent_id = os.getenv("RETELL_AGENT_ID")
    if agent_id:
        body["call_inbound"]["override_agent_id"] = agent_id
    ver = os.getenv("RETELL_AGENT_VERSION", "").strip()
    if ver.isdigit():
        body["call_inbound"]["override_agent_version"] = int(ver)
    if user_id is not None:
        settings = None
        try:
            settings = db.get_inbound_call_settings(user_id)
        except Exception:
            settings = None

        tz_name = (os.getenv("RETELL_INBOUND_TIMEZONE") or "UTC").strip()
        try:
            from zoneinfo import ZoneInfo

            now = datetime.now(ZoneInfo(tz_name))
            tz_label = tz_name
        except Exception:
            now = datetime.now(timezone.utc)
            tz_label = "UTC"
        dv = {
            "user_id": str(user_id),
            "agent_name": (settings or {}).get("agent_name") or "SUMA",
            "business_name": (settings or {}).get("business_name") or "our business",
            "call_context": (settings or {}).get("call_context") or "",
            # Current date/time for prompts (no separate tool call needed).
            "current_date": now.strftime("%Y-%m-%d"),
            "iso_date": now.date().isoformat(),
            "current_time": now.strftime("%H:%M"),
            "day_of_week": now.strftime("%A"),
            "timezone": tz_label,
        }
        # Response engine: dynamic_variables (general) + retell_llm_dynamic_variables (Retell LLM)
        body["call_inbound"]["dynamic_variables"] = dv
        body["call_inbound"]["retell_llm_dynamic_variables"] = dv
    return JSONResponse(status_code=200, content=body)


@router.get("/retell-health")
async def retell_health():
    """Quick prod check: API key present (does not call Retell)."""
    ok = bool(os.getenv("RETELL_API_KEY"))
    return JSONResponse(
        status_code=200 if ok else 503,
        content={
            "retell_api_key_configured": ok,
            "retell_agent_id_configured": bool(os.getenv("RETELL_AGENT_ID")),
            "inbound_number_configured": bool(
                os.getenv("RETELL_INBOUND_NUMBER") or os.getenv("RETELL_PUBLIC_INBOUND_NUMBER")
            ),
            "default_inbound_user_configured": bool(os.getenv("RETELL_DEFAULT_INBOUND_USER_ID")),
        },
    )


class DashboardReportSummary(BaseModel):
    total_calls: int
    total_appointments: int
    total_minutes: float
    successful_calls: int
    unanswered_calls: int = 0
    repeat_callers: int
    new_callers: int
    appointment_status_distribution: dict = {}


class DashboardRepeatCallerItem(BaseModel):
    phone: str
    name: str
    call_count: int


class DashboardReportResponse(BaseModel):
    period_days: int
    summary: DashboardReportSummary
    calls_over_time: list[dict]
    appointments_over_time: list[dict]
    top_repeat_callers: list[DashboardRepeatCallerItem]
    sentiment_breakdown: dict


@router.get("/dashboard/reports", response_model=DashboardReportResponse)
async def dashboard_reports(
    days: int = Query(7, ge=1, le=366),
    user=Depends(get_current_user),
):
    """Time-series and aggregates from call_history + appointments (for charts)."""
    uid = user["id"]
    summary_raw = db.get_dashboard_summary_stats(uid, days)
    summary = DashboardReportSummary(
        total_calls=summary_raw["total_calls"],
        total_appointments=summary_raw["total_appointments_in_period"],
        total_minutes=summary_raw["total_minutes"],
        successful_calls=summary_raw["successful_calls"],
        unanswered_calls=summary_raw["unanswered_calls"],
        repeat_callers=summary_raw["repeat_callers"],
        new_callers=summary_raw["new_callers"],
        appointment_status_distribution=summary_raw.get("appointment_status_distribution") or {},
    )
    calls_over_time = db.get_calls_over_time(uid, days)
    appointments_over_time = db.get_appointments_over_time(uid, days)
    top = db.get_top_repeat_callers(uid, days, limit=10)
    sentiment = db.get_sentiment_breakdown(uid, days)
    return DashboardReportResponse(
        period_days=days,
        summary=summary,
        calls_over_time=calls_over_time,
        appointments_over_time=appointments_over_time,
        top_repeat_callers=[DashboardRepeatCallerItem(**x) for x in top],
        sentiment_breakdown=sentiment,
    )


@router.get("/dashboard/stats")
async def dashboard_quick_stats(user=Depends(get_current_user)):
    """Compact counters for header widgets."""
    return JSONResponse(db.get_dashboard_combined_stats(user["id"]))


@router.get("/dashboard/appointments")
async def dashboard_appointments(
    all_time: bool = Query(
        True,
        description="When true (default), return every appointment in one response (newest first, capped at 5000). When false, use from_date + pagination.",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    from_date: str | None = Query(None, description="YYYY-MM-DD (used only when all_time=false; defaults to today UTC)"),
    user=Depends(get_current_user),
):
    """
    Single endpoint for the frontend:
    totals plus the full appointment list (caller phone and linked call_id when stored).
    """
    data = db.get_user_appointments_dashboard(
        user_id=user["id"],
        page=page,
        page_size=page_size,
        from_date=from_date,
        all_time=all_time,
    )

    out_appts: list[dict] = []
    for a in data.get("appointments", []) or []:
        if not isinstance(a, dict):
            continue
        out_appts.append(
            {
                "id": a.get("id"),
                "appointment_date": a.get("appointment_date"),
                "start_time": a.get("start_time"),
                "end_time": a.get("end_time"),
                "status": a.get("status"),
                "title": a.get("title"),
                "description": a.get("description"),
                "notes": a.get("notes"),
                "created_at": a.get("created_at"),
                "caller_name": a.get("attendee_name"),
                "caller_email": a.get("attendee_email"),
                "caller_phone": a.get("caller_phone"),
                "call_id": a.get("call_id"),
            }
        )

    payload: dict = {
        "totals": data.get("totals") or {"total": 0, "scheduled": 0, "cancelled": 0, "completed": 0},
        "appointments": out_appts,
    }
    if not all_time:
        payload["page"] = data.get("page", page)
        payload["page_size"] = data.get("page_size", page_size)

    return JSONResponse(content=jsonable_encoder(payload))


@router.get("/dashboard/calls/{call_id}")
async def dashboard_call_detail(
    call_id: str,
    user=Depends(get_current_user),
):
    """
    Dashboard call detail (compact).

    Intentionally returns ONLY the fields needed by the UI:
    - call number(s), times, duration
    - recording_url
    - transcript (clean text)
    - summary, sentiment, booking_done
    """
    row = db.get_call_dashboard_detail(call_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Call not found")

    def _transcript_to_text(t) -> str | None:
        if not t:
            return None
        # Already plain text
        if isinstance(t, str):
            return t.strip() or None
        if isinstance(t, dict):
            # our wrapper formats
            if isinstance(t.get("transcript_text"), str):
                return t["transcript_text"].strip() or None
            tw = t.get("transcript_with_tool_calls")
            if isinstance(tw, list):
                parts: list[str] = []
                for item in tw:
                    if not isinstance(item, dict):
                        continue
                    role = item.get("role")
                    content = item.get("content")
                    if isinstance(content, str) and content.strip():
                        prefix = "Agent" if role == "agent" else ("Caller" if role == "user" else None)
                        parts.append(f"{prefix + ': ' if prefix else ''}{content.strip()}")
                return "\n".join(parts).strip() or None
            tobj = t.get("transcript_object")
            if isinstance(tobj, list):
                parts: list[str] = []
                for item in tobj:
                    if not isinstance(item, dict):
                        continue
                    role = item.get("role")
                    content = item.get("content")
                    if isinstance(content, str) and content.strip():
                        prefix = "Agent" if role == "agent" else ("Caller" if role == "user" else None)
                        parts.append(f"{prefix + ': ' if prefix else ''}{content.strip()}")
                return "\n".join(parts).strip() or None
        return None

    transcript_obj = row.get("transcript")
    transcript_text = _transcript_to_text(transcript_obj)

    # ---- Extract a couple fields from the stored summary (Retell call_analyzed) ----
    # summary format is free-form text (often includes "Sentiment: X" and "Custom analysis: {...json...}")
    def _extract_sentiment_and_booking(summary: Any) -> tuple[Optional[str], Optional[bool]]:
        if not isinstance(summary, str) or not summary.strip():
            return None, None
        s = summary
        sentiment: Optional[str] = None
        booking_done: Optional[bool] = None
        try:
            for line in s.splitlines():
                if line.lower().startswith("sentiment:"):
                    sentiment = line.split(":", 1)[1].strip() or None
                    break
        except Exception:
            sentiment = None
        try:
            # Look for "Custom analysis: { ... }" JSON blob
            marker = "Custom analysis:"
            idx = s.find(marker)
            if idx != -1:
                blob = s[idx + len(marker) :].strip()
                # The JSON is usually the last part; parse best-effort.
                try:
                    custom = json.loads(blob)
                except Exception:
                    # Sometimes there is trailing text; try to trim to last closing brace.
                    end = blob.rfind("}")
                    if end != -1:
                        custom = json.loads(blob[: end + 1])
                    else:
                        custom = None
                if isinstance(custom, dict) and "appointment_booked" in custom:
                    booking_done = bool(custom.get("appointment_booked"))
        except Exception:
            booking_done = None
        return sentiment, booking_done

    sentiment, booking_done = _extract_sentiment_and_booking(row.get("summary"))

    compact: dict[str, Any] = {
        "call_id": row.get("call_id") or call_id,
        "status": row.get("status"),
        "caller_phone": row.get("from_number"),
        "agent_phone": row.get("to_number"),
        "started_at": row.get("started_at"),
        "ended_at": row.get("ended_at"),
        "duration": row.get("duration"),
        "recording_url": row.get("recording_url"),
        "transcript": transcript_text,
        "summary": row.get("summary"),
        "sentiment": sentiment,
        "booking_done": booking_done,
    }
    return JSONResponse(content=jsonable_encoder(compact))


@router.get("/dashboard/calls/{call_id}/debug")
async def dashboard_call_detail_debug(
    call_id: str,
    user=Depends(get_current_user),
):
    """
    Debug endpoint: returns the full stored call row including webhook trails.
    Use this only when troubleshooting Retell payloads.
    """
    row = db.get_call_dashboard_detail(call_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Call not found")
    return JSONResponse(content=jsonable_encoder(row))


@router.get("/dashboard/calls/{call_id}/live")
async def dashboard_call_live(call_id: str, user=Depends(get_current_user)):
    """Live call payload from Retell (for active calls or post-mortem metadata)."""
    row = db.get_call_by_id(call_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Call not found")
    try:
        payload = retell_get_call(call_id)
    except Exception as e:
        logging.error("Retell get-call failed for %s: %s", call_id, e)
        raise HTTPException(status_code=502, detail=str(e))
    return JSONResponse(content=payload)


#
# NOTE: Retell agent prompt endpoints removed (UI only edits flow prompt + voice).
#


@router.get("/retell/voices")
async def retell_get_voices(user=Depends(get_current_user)):
    """
    List voices from Retell + current agent voice_id for UI selection.

    Filters applied (per UI requirement):
    - provider: ElevenLabs (voice_id prefix '11labs-')
    - gender: female
    - age: middle-aged
    - language: english or spanish (if Retell provides language metadata)
    """
    agent_id = (os.getenv("RETELL_AGENT_ID") or "").strip()
    if not agent_id:
        raise HTTPException(status_code=500, detail="RETELL_AGENT_ID is not configured")
    try:
        agent = retell_get_agent(agent_id)
        voices = retell_list_voices()
    except Exception as e:
        logging.error("Retell voices fetch failed: %s", e)
        raise HTTPException(status_code=502, detail=str(e))

    def _norm(s: object) -> str:
        return str(s or "").strip().lower()

    def _is_11labs(v: dict) -> bool:
        vid = _norm(v.get("voice_id"))
        prov = _norm(v.get("provider"))
        return vid.startswith("11labs-") or prov in ("11labs", "elevenlabs")

    allowed_lang = {"en", "eng", "english", "es", "spa", "spanish"}
    filtered: list[dict] = []
    for v in voices:
        if not isinstance(v, dict):
            continue
        if not _is_11labs(v):
            continue
        if _norm(v.get("gender")) != "female":
            continue
        # Retell may use "middle_aged" or "middle-aged" or "middle aged"
        age = _norm(v.get("age")).replace("_", " ").replace("-", " ")
        if age not in ("middle aged", "middleage", "middle"):
            # tolerate a few variants
            if "middle" not in age:
                continue
        lang = _norm(v.get("language") or v.get("lang"))
        # If language isn't provided by Retell, allow it through; otherwise enforce EN/ES
        if lang and lang not in allowed_lang:
            continue

        filtered.append(
            {
                "voice_id": v.get("voice_id"),
                "voice_name": v.get("voice_name"),
                "provider": v.get("provider"),
                "gender": v.get("gender"),
                "age": v.get("age"),
                "accent": v.get("accent"),
                "language": v.get("language") or v.get("lang"),
                "preview_audio_url": v.get("preview_audio_url"),
            }
        )

    return JSONResponse(
        content=jsonable_encoder(
            {
                "current_voice_id": agent.get("voice_id"),
                "voices": filtered,
            }
        )
    )


class RetellAgentVoiceUpdate(BaseModel):
    voice_id: str


@router.put("/retell/agent/voice")
async def retell_put_agent_voice(
    body: RetellAgentVoiceUpdate,
    user=Depends(get_current_user),
    version: int | None = Query(None, ge=0),
):
    """Update the configured Retell agent voice_id."""
    agent_id = (os.getenv("RETELL_AGENT_ID") or "").strip()
    if not agent_id:
        raise HTTPException(status_code=500, detail="RETELL_AGENT_ID is not configured")
    voice_id = (body.voice_id or "").strip()
    if not voice_id:
        raise HTTPException(status_code=400, detail="voice_id must be non-empty")
    try:
        payload = retell_update_agent(agent_id, updates={"voice_id": voice_id}, version=version)
    except Exception as e:
        logging.error("Retell update-agent(voice) failed for agent_id=%s: %s", agent_id, e)
        raise HTTPException(status_code=502, detail=str(e))
    return JSONResponse(content=payload)


@router.get("/retell/flow/editor")
async def retell_get_flow_editor(user=Depends(get_current_user), version: int | None = Query(None, ge=0)):
    """
    Compact editor payload for UI:
    {conversation_flow_id, version, global_prompt, intro_node_id, intro_text}
    """
    flow_id = (os.getenv("RETELL_CONVERSATION_FLOW_ID") or "").strip()
    if not flow_id:
        raise HTTPException(status_code=500, detail="RETELL_CONVERSATION_FLOW_ID is not configured")
    try:
        flow = retell_get_conversation_flow(flow_id, version=version)
    except Exception as e:
        logging.error("Retell get-conversation-flow(editor) failed for flow_id=%s: %s", flow_id, e)
        raise HTTPException(status_code=502, detail=str(e))

    intro_id = flow.get("start_node_id")
    intro_text = None
    nodes = flow.get("nodes") or []
    if intro_id and isinstance(nodes, list):
        for n in nodes:
            if not isinstance(n, dict):
                continue
            if str(n.get("id")) == str(intro_id):
                instr = n.get("instruction") or {}
                if isinstance(instr, dict):
                    intro_text = instr.get("text")
                break

    return JSONResponse(
        content=jsonable_encoder(
            {
                "conversation_flow_id": flow.get("conversation_flow_id") or flow_id,
                "version": flow.get("version"),
                "global_prompt": flow.get("global_prompt"),
                "intro_node_id": intro_id,
                "intro_text": intro_text,
            }
        )
    )


class RetellFlowPromptAndIntroUpdate(BaseModel):
    global_prompt: str | None = None
    intro_node_id: str = "start"
    intro_text: str | None = None


@router.put("/retell/flow/prompt-and-intro")
async def retell_put_flow_prompt_and_intro(
    body: RetellFlowPromptAndIntroUpdate,
    user=Depends(get_current_user),
    version: int | None = Query(None, ge=0),
):
    """
    Convenience endpoint:
    - updates conversation_flow.global_prompt
    - updates a single node's instruction.text (intro node) by node id
    """
    flow_id = (os.getenv("RETELL_CONVERSATION_FLOW_ID") or "").strip()
    if not flow_id:
        raise HTTPException(status_code=500, detail="RETELL_CONVERSATION_FLOW_ID is not configured")

    updates: dict = {}
    if body.global_prompt is not None:
        gp = body.global_prompt.strip()
        updates["global_prompt"] = gp

    intro_text = body.intro_text.strip() if body.intro_text is not None else None
    intro_node_id = (body.intro_node_id or "").strip() or "start"

    # Update intro node by fetching the full flow and patching nodes safely.
    if intro_text is not None:
        try:
            current = retell_get_conversation_flow(flow_id, version=version)
        except Exception as e:
            logging.error("Retell get-conversation-flow (for node update) failed for %s: %s", flow_id, e)
            raise HTTPException(status_code=502, detail=str(e))
        nodes = current.get("nodes") or []
        if not isinstance(nodes, list) or not nodes:
            raise HTTPException(status_code=400, detail="Conversation flow has no nodes to update")
        found = False
        new_nodes: list[dict] = []
        for n in nodes:
            if not isinstance(n, dict):
                continue
            if str(n.get("id")) == intro_node_id:
                nn = dict(n)
                instr = nn.get("instruction") or {}
                if not isinstance(instr, dict):
                    instr = {}
                instr2 = dict(instr)
                instr2["type"] = instr2.get("type") or "prompt"
                instr2["text"] = intro_text
                nn["instruction"] = instr2
                new_nodes.append(nn)
                found = True
            else:
                new_nodes.append(n)
        if not found:
            raise HTTPException(status_code=404, detail=f"Intro node '{intro_node_id}' not found in flow")
        updates["nodes"] = new_nodes

    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    try:
        payload = retell_update_conversation_flow(flow_id, updates=updates, version=version)
    except Exception as e:
        logging.error("Retell update-conversation-flow(prompt-and-intro) failed for flow_id=%s: %s", flow_id, e)
        raise HTTPException(status_code=502, detail=str(e))
    return JSONResponse(content=payload)


# @router.get("/call-history")
# async def get_user_call_history(
#     page: int = Query(1, ge=1),
#     page_size: int = Query(10, ge=1, le=100),
#     user = Depends(get_current_user)
# ):
#     """
#     Get call history with parsed transcripts showing only the conversation text
#     """
#     try:
#         call_history = db.get_call_history_by_user_id(user["id"], page, page_size)
        
#         # Process each call to include formatted transcript
#         processed_calls = []
#         for call in call_history["calls"]:
#             call_data = {**call}
            
#             # Parse and extract transcript text
#             transcript_text = None
#             if call.get("transcript"):
#                 try:
#                     transcript_data = call["transcript"]
                    
#                     # If transcript is a string, parse it
#                     if isinstance(transcript_data, str):
#                         transcript_data = json.loads(transcript_data)
                    
#                     # Extract conversation as plain text
#                     conversation_lines = []
#                     if isinstance(transcript_data, list):
#                         for item in transcript_data:
#                             if item.get("type") == "message":
#                                 role = item.get("role", "unknown")
#                                 content = item.get("content", [])
                                
#                                 # Handle content as list or string
#                                 if isinstance(content, list):
#                                     text = " ".join(str(c) for c in content)
#                                 else:
#                                     text = str(content)
                                
#                                 # Format: "Assistant: Hello there"
#                                 speaker = "Assistant" if role == "assistant" else "User"
#                                 conversation_lines.append(f"{speaker}: {text}")
                    
#                     transcript_text = "\n".join(conversation_lines) if conversation_lines else None
                    
#                 except Exception as e:
#                     logging.warning(f"Error parsing transcript for call {call.get('id')}: {e}")
#                     transcript_text = None
            
#             call_data["transcript"] = transcript_text
#             processed_calls.append(call_data)
        
#         return JSONResponse(content=jsonable_encoder({
#             "user_id": user["id"],
#             "pagination": {
#                 "page": call_history["page"],
#                 "page_size": call_history["page_size"],
#                 "total": call_history["total"],
#                 "completed_calls": call_history["completed_calls"],
#                 "not_completed_calls": call_history["not_completed_calls"]
#             },
#             "calls": processed_calls
#         }))
#     except Exception as e:
#         logging.error(f"Error fetching call history: {e}")
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=f"Error fetching call history: {str(e)}")


# In routes.py - Update get_call_status endpoint


#
# /call-status/{call_id} removed. Use /dashboard/calls/{call_id}.
#
                            

@router.get("/call-history")
async def get_user_call_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, le=100),
    user=Depends(get_current_user)
):
    try:
        def _transcript_to_text(t) -> str | None:
            if not t:
                return None
            if isinstance(t, str):
                return t.strip() or None
            if isinstance(t, dict):
                if isinstance(t.get("transcript_text"), str):
                    return t["transcript_text"].strip() or None
                tw = t.get("transcript_with_tool_calls")
                if isinstance(tw, list):
                    parts: list[str] = []
                    for item in tw:
                        if not isinstance(item, dict):
                            continue
                        role = item.get("role")
                        content = item.get("content")
                        if isinstance(content, str) and content.strip():
                            prefix = "Agent" if role == "agent" else ("Caller" if role == "user" else None)
                            parts.append(f"{prefix + ': ' if prefix else ''}{content.strip()}")
                    return "\n".join(parts).strip() or None
                tobj = t.get("transcript_object")
                if isinstance(tobj, list):
                    parts: list[str] = []
                    for item in tobj:
                        if not isinstance(item, dict):
                            continue
                        role = item.get("role")
                        content = item.get("content")
                        if isinstance(content, str) and content.strip():
                            prefix = "Agent" if role == "agent" else ("Caller" if role == "user" else None)
                            parts.append(f"{prefix + ': ' if prefix else ''}{content.strip()}")
                    return "\n".join(parts).strip() or None
            if isinstance(t, list):
                # legacy list format (LiveKit-style)
                lines: list[str] = []
                for msg in t:
                    if not isinstance(msg, dict):
                        continue
                    if msg.get("type") == "message":
                        speaker = "Assistant" if msg.get("role") == "assistant" else "User"
                        content = msg.get("content")
                        text = " ".join(content) if isinstance(content, list) else (str(content) if content is not None else "")
                        if text.strip():
                            lines.append(f"{speaker}: {text.strip()}")
                return "\n".join(lines).strip() or None
            return None

        history = db.get_call_history_by_user_id(user["id"], page, page_size)

        calls = []
        for call in history.get("calls", []):
            # Proper transcript text (Retell + legacy formats)
            transcript_text = None
            if call.get("transcript"):
                try:
                    tr = call["transcript"]
                    if isinstance(tr, str):
                        try:
                            tr = json.loads(tr)
                        except Exception:
                            pass
                    transcript_text = _transcript_to_text(tr)
                except Exception as e:
                    logging.warning(f"Transcript parse error for {call.get('id')}: {e}")

            calls.append(
                {
                    "call_id": call.get("call_id"),
                    "status": call.get("status"),
                    "caller_phone": call.get("from_number"),
                    "agent_phone": call.get("to_number"),
                    "started_at": call.get("started_at"),
                    "ended_at": call.get("ended_at"),
                    "duration": call.get("duration"),
                    "recording_url": call.get("recording_url"),
                    "summary": call.get("summary"),
                    "transcript_text": transcript_text,
                }
            )

        # Build pagination block safely
        pagination = history.get("pagination") or {
            "page": history.get("page", page),
            "page_size": history.get("page_size", page_size),
            "total": history.get("total", len(calls)),
            "completed_calls": history.get("completed_calls", 0),
            "not_completed_calls": history.get("not_completed_calls", 0),
        }

        from fastapi.encoders import jsonable_encoder

        return JSONResponse(content=jsonable_encoder({
            "user_id": user["id"],
            "pagination": pagination,
            "calls": calls
        }))

    except Exception as e:
        logging.error(f"Error fetching history: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


#
# /call-history/{call_id} removed. Use /dashboard/calls/{call_id}.
#


# @router.post("/agent/check-availability")
# async def check_availability(request: Request):
#     """
#     API for LiveKit agent to check if a time slot is available
#     """
#     try:
#         data = await request.json()
        
#         user_id = data.get("user_id")
#         appointment_date = data.get("appointment_date")
#         start_time = data.get("start_time")
#         end_time = data.get("end_time")
        
#         has_conflict = db.check_appointment_conflict(
#             user_id=user_id,
#             appointment_date=appointment_date,
#             start_time=start_time,
#             end_time=end_time
#         )
        
#         return JSONResponse({
#             "success": True,
#             "available": not has_conflict,
#             "message": "Time slot available" if not has_conflict else "Time slot already booked"
#         })
        
#     except Exception as e:
#         logging.error(f"Error checking availability: {e}")
#         return error_response(f"Failed to check availability: {str(e)}", status_code=500)


#
# /api/agent/* routes removed (LiveKit/legacy tool endpoints).
#



#
# /agent/save-call-data and /agent/report-event removed. Retell webhooks persist call state.
#
    








from pydantic import BaseModel, Field

class PromptCustomizationUpdate(BaseModel):
    system_prompt: str = Field(..., min_length=10, max_length=10000)


@router.get("/dashboard/prompt")
async def dashboard_get_prompt(user=Depends(get_current_user)):
    """Compact prompt read: system_prompt and updated_at only."""
    prompt_data = db.get_user_prompt(user["id"])
    if not prompt_data:
        return error_response("Prompt not found", status_code=404)
    return JSONResponse(
        content=jsonable_encoder(
            {
                "system_prompt": prompt_data["system_prompt"],
                "updated_at": prompt_data["updated_at"],
            }
        )
    )


@router.put("/dashboard/prompt")
async def dashboard_put_prompt(
    customization: PromptCustomizationUpdate,
    user=Depends(get_current_user),
):
    """Compact prompt update: returns system_prompt and updated_at only."""
    prompt_text = customization.system_prompt.strip()
    if not prompt_text:
        return error_response("System prompt cannot be empty", status_code=400)
    updated_prompt = db.update_user_system_prompt(
        user_id=user["id"],
        system_prompt=prompt_text,
    )
    if not updated_prompt:
        return error_response("Failed to update prompt", status_code=500)
    return JSONResponse(
        content=jsonable_encoder(
            {
                "system_prompt": updated_prompt["system_prompt"],
                "updated_at": updated_prompt["updated_at"],
            }
        )
    )


@router.get("/prompt-customization")
async def get_prompt_customization(user=Depends(get_current_user)):
    """
    Get the user's complete system prompt as plain text.
    No field parsing - returns exactly what's stored.
    """
    try:
        prompt_data = db.get_user_prompt(user["id"])
        
        if not prompt_data:
            return error_response("Prompt not found", status_code=404)
        
        # Just return the system_prompt field directly
        return JSONResponse(content=jsonable_encoder({
            "success": True,
            "system_prompt": prompt_data["system_prompt"],  # Single field from DB
            "updated_at": prompt_data["updated_at"]
        }))
        
    except Exception as e:
        logging.error(f"Error fetching prompt customization: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/prompt-customization")
async def update_prompt_customization(
    customization: PromptCustomizationUpdate,
    user=Depends(get_current_user)
):
    """
    Update user's system prompt.
    Stores exactly what user sends - no parsing.
    """
    try:
        prompt_text = customization.system_prompt.strip()
        
        if not prompt_text:
            return error_response("System prompt cannot be empty", status_code=400)
        
        # Just update the single system_prompt field
        updated_prompt = db.update_user_system_prompt(
            user_id=user["id"],
            system_prompt=prompt_text  # Store as-is
        )
        
        if not updated_prompt:
            return error_response("Failed to update customization", status_code=500)
        
        return JSONResponse(content=jsonable_encoder({
            "success": True,
            "message": "Prompt customization updated successfully",
            "system_prompt": updated_prompt["system_prompt"],
            "updated_at": updated_prompt["updated_at"]
        }))
        
    except Exception as e:
        logging.error(f"Error updating prompt customization: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/prompt-customization/reset")
async def reset_prompt_customization(user=Depends(get_current_user)):
    """
    Reset user's system prompt to default text.
    """
    try:
        reset_prompt = db.reset_user_prompt_to_default(user["id"])
        
        if not reset_prompt:
            return error_response("Failed to reset customization", status_code=500)
        
        return JSONResponse(content=jsonable_encoder({
            "success": True,
            "message": "Prompt customization reset to defaults",
            "system_prompt": reset_prompt["system_prompt"],  # Default text
            "updated_at": reset_prompt["updated_at"]
        }))
        
    except Exception as e:
        logging.error(f"Error resetting prompt customization: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

#
# /calls/{call_id}/recording/stream and /calls/{call_id}/transcript removed.
# Use /dashboard/calls/{call_id} for transcript_text and recording_url.
#