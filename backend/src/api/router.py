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
)

load_dotenv()

router = APIRouter()
mail_obj = Send_Mail()
db = PGDB()
db.create_retell_webhook_dedupe_table()
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


@router.get("/dashboard/calls/{call_id}")
async def dashboard_call_detail(call_id: str, user=Depends(get_current_user)):
    """Call row plus Retell webhook trail (events_log, agent_events)."""
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


@router.get("/agent/get-current-datetime")
async def agent_get_current_datetime():
    """
    Legacy: prefer inbound webhook dynamic variables (current_date, current_time, …) in the
    Retell prompt instead of a tool call. Remove the tool from your Conversation Flow when migrated.
    """
    now = datetime.now(timezone.utc)
    return JSONResponse(
        {
            "current_date": now.strftime("%Y-%m-%d"),
            "iso_date": now.date().isoformat(),
            "current_time": now.strftime("%H:%M"),
            "day_of_week": now.strftime("%A"),
            "timezone": "UTC",
        }
    )


# Backward-compat: your Retell tool config currently uses POST for get_current_datetime.
@router.post("/agent/get-current-datetime")
async def agent_get_current_datetime_post():
    return await agent_get_current_datetime()


class CheckAvailabilityRequest(BaseModel):
    user_id: int | None = None
    appointment_date: str  # YYYY-MM-DD
    start_time: str  # HH:MM
    end_time: str | None = None  # HH:MM


@router.post("/agent/check-availability")
async def agent_check_availability(body: CheckAvailabilityRequest):
    """
    Checks for conflicts in the appointments table.
    Returns {available: bool, message: str, conflict_details?: {...}}
    """
    try:
        logging.info(
            "check-availability: raw user_id=%r appointment_date=%r start_time=%r end_time=%r",
            body.user_id,
            getattr(body, "appointment_date", None),
            getattr(body, "start_time", None),
            getattr(body, "end_time", None),
        )
        uid = resolve_agent_tool_user_id(body.user_id)
        if uid is None:
            logging.error(
                "check-availability: could not resolve user_id. raw=%r env=%r",
                body.user_id,
                os.getenv("RETELL_DEFAULT_INBOUND_USER_ID"),
            )
            return JSONResponse(
                status_code=200,
                content={
                    "available": True,
                    "message": "Could not resolve business user for this call.",
                    "warning": "missing_user_id",
                },
            )
        logging.info("check-availability: resolved user_id=%s", uid)
        # Validate user exists (prevents FK / bad mapping issues downstream)
        try:
            if not db.get_user_by_id(uid):
                logging.error("check-availability: user not found for user_id=%s", uid)
                return JSONResponse(
                    status_code=200,
                    content={
                        "available": True,
                        "message": "I couldn't verify availability for this account right now.",
                        "warning": "invalid_user_id_mapping",
                    },
                )
        except Exception:
            pass
        appt_date = body.appointment_date
        start = body.start_time
        end = body.end_time
        if not end:
            # default to 1h duration
            start_dt = datetime.strptime(start, "%H:%M")
            end = (start_dt + timedelta(hours=1)).strftime("%H:%M")
        logging.info("check-availability: query user_id=%s date=%s start=%s end=%s", uid, appt_date, start, end)

        row = None
        for attempt in range(2):
            conn = db.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT appointment_date, start_time, end_time, title, attendee_name
                        FROM appointments
                        WHERE user_id = %s
                          AND appointment_date = %s
                          AND status = 'scheduled'
                          AND (start_time < %s::time AND end_time > %s::time)
                        ORDER BY start_time
                        LIMIT 1
                        """,
                        (uid, appt_date, end, start),
                    )
                    row = cursor.fetchone()
                break
            except Exception as e:
                logging.error("check-availability error: %s", e)
                try:
                    conn.rollback()
                except Exception:
                    pass
                if attempt == 0:
                    continue
                row = None
            finally:
                db.release_connection(conn)

        if row:
            logging.info("check-availability: conflict found for user_id=%s date=%s", uid, appt_date)
            conflict_details = {
                "appointment_date": str(row[0]),
                "start_time": str(row[1]),
                "end_time": str(row[2]),
                "title": row[3],
                "attendee_name": row[4],
            }
            return JSONResponse(
                {
                    "available": False,
                    "message": "That time is already booked. Please pick another slot.",
                    "conflict_details": conflict_details,
                }
            )

        return JSONResponse(
            {"available": True, "message": "That time is available. You can book it."}
        )
    except Exception as e:
        logging.error("check-availability error: %s", e)
        return JSONResponse(
            status_code=200,
            content={
                "available": True,
                "message": "I couldn't verify availability right now, but you can proceed with booking.",
                "warning": str(e),
            },
        )


class SendConfirmationRequest(BaseModel):
    user_id: int | None = None
    appointment_id: str
    appointment_date: str
    start_time: str
    attendee_name: str
    organizer_email: str | None = None
    notes: str | None = None


@router.post("/agent/send-confirmation")
async def agent_send_confirmation(body: SendConfirmationRequest):
    """
    Sends a confirmation email. If email isn't available, returns sent=false.
    """
    try:
        if not body.organizer_email:
            return JSONResponse({"sent": False, "message": "No email provided"})

        title = "Appointment Confirmation"
        description = "Appointment booked via inbound call by SUMA."
        end_time = None
        try:
            st = datetime.strptime(body.start_time, "%H:%M")
            end_time = (st + timedelta(hours=1)).strftime("%H:%M")
        except Exception:
            end_time = body.start_time

        sent = await mail_obj.send_email_with_calendar_event(
            attendee_email=body.organizer_email,
            attendee_name=body.attendee_name,
            appointment_date=body.appointment_date,
            start_time=body.start_time,
            end_time=end_time,
            title=title,
            description=description,
            organizer_name=body.attendee_name,
            organizer_email=body.organizer_email,
        )
        return JSONResponse({"sent": bool(sent), "message": "Confirmation processed"})
    except Exception as e:
        logging.error("send-confirmation error: %s", e)
        return JSONResponse(
            status_code=200,
            content={"sent": False, "message": "Failed to send confirmation", "error": str(e)},
        )


@router.post("/livekit-webhook")
async def livekit_webhook(request: Request):
    raise HTTPException(
        status_code=410,
        detail="LiveKit webhooks removed (Retell is now the calling provider). Use /api/retell-webhook.",
    )



    


@router.post("/livekit-egress-webhook")
async def livekit_egress_webhook(request: Request):
    raise HTTPException(
        status_code=410,
        detail="LiveKit egress webhooks removed (Retell is now the calling provider).",
    )


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


@router.get("/call-status/{call_id}")
async def get_call_status(call_id: str):
    """Optimized status check with proper connection handling"""
    try:
        conn = db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT status, created_at, ended_at, duration, started_at
                    FROM call_history 
                    WHERE call_id = %s
                """, (call_id,))
                row = cursor.fetchone()
        finally:
            db.release_connection(conn)  # ✅ FIXED: Was conn.close()
        
        if not row:
            return JSONResponse(
                status_code=404,
                content={"status": "not_found", "is_final": True}
            )
        
        current_status, created_at, ended_at, duration, started_at = row
        
        # ✅ Normalize status
        if current_status not in {"initialized", "dialing", "connected", "completed", "unanswered"}:
            STATUS_MAP = {
                "initiated": "initialized",
                "in_progress": "connected",
                "failed": "unanswered",
                "not_attended": "unanswered"
            }
            current_status = STATUS_MAP.get(current_status, "initialized")
        
        # Calculate elapsed time
        time_elapsed = 0
        if created_at:
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            time_elapsed = (datetime.now(timezone.utc) - created_at).total_seconds()
        
        is_final = current_status in {"completed", "unanswered"}
        
        response = {
            "status": current_status,
            "message": {
                "initialized": "Initializing...",
                "dialing": "Dialing...",
                "connected": "Call in progress",
                "completed": "Call completed",
                "unanswered": "Call not answered"
            }.get(current_status, current_status),
            "time_elapsed": round(time_elapsed, 1),
            "is_final": is_final
        }
        
        if is_final and duration:
            response["duration"] = round(duration, 1)
        
        if started_at:
            response["started_at"] = started_at.isoformat()
        if ended_at:
            response["ended_at"] = ended_at.isoformat()
        
        return JSONResponse(response)
        
    except Exception as e:
        logging.error(f"get_call_status error: {e}")
        return JSONResponse(
            {"status": "error", "message": str(e), "is_final": True},
            status_code=500
        )
                            

@router.get("/call-history")
async def get_user_call_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, le=100),
    user=Depends(get_current_user)
):
    try:
        history = db.get_call_history_by_user_id(user["id"], page, page_size)

        calls = []
        for call in history.get("calls", []):
            call_data = {**call}
            
            # ✅ FIX 1: Ensure timestamps are included and properly formatted
            # Convert datetime objects to ISO strings if they exist
            if call.get("created_at"):
                call_data["created_at"] = call["created_at"].isoformat() if hasattr(call["created_at"], 'isoformat') else str(call["created_at"])
            
            if call.get("started_at"):
                call_data["started_at"] = call["started_at"].isoformat() if hasattr(call["started_at"], 'isoformat') else str(call["started_at"])
            
            if call.get("ended_at"):
                call_data["ended_at"] = call["ended_at"].isoformat() if hasattr(call["ended_at"], 'isoformat') else str(call["ended_at"])
            
            # ✅ FIX 2: Calculate display duration if not available
            if not call_data.get("duration") and call.get("started_at") and call.get("ended_at"):
                try:
                    from datetime import datetime
                    start = call["started_at"] if isinstance(call["started_at"], datetime) else datetime.fromisoformat(str(call["started_at"]))
                    end = call["ended_at"] if isinstance(call["ended_at"], datetime) else datetime.fromisoformat(str(call["ended_at"]))
                    call_data["duration"] = round((end - start).total_seconds(), 1)
                except:
                    call_data["duration"] = 0
            
            # Parse transcript text
            transcript_text = None
            if call.get("transcript"):
                try:
                    tr = call["transcript"]
                    if isinstance(tr, str):
                        tr = json.loads(tr)
                    if isinstance(tr, list):
                        lines = []
                        for msg in tr:
                            if msg.get("type") == "message":
                                speaker = "Assistant" if msg.get("role") == "assistant" else "User"
                                text = " ".join(msg.get("content", [])) if isinstance(msg.get("content"), list) else str(msg.get("content"))
                                lines.append(f"{speaker}: {text}")
                        transcript_text = "\n".join(lines)
                except Exception as e:
                    logging.warning(f"Transcript parse error for {call.get('id')}: {e}")
            
            call_data["transcript_text"] = transcript_text
            
            # ✅ FIX 3: Add recording availability flag
            call_data["has_recording"] = bool(call.get("recording_url") or call.get("recording_blob_data"))

            # ---- Reference-style fields (additive; keeps existing keys intact) ----
            call_data["call_status"] = call_data.get("status")
            call_data["caller_phone"] = call_data.get("from_number")
            call_data["agent_phone"] = call_data.get("to_number")
            call_data["direction"] = "inbound"  # current system is inbound-first
            call_data["call_summary"] = call_data.get("summary")
            if call_data.get("duration") is not None:
                try:
                    call_data["duration_ms"] = int(float(call_data["duration"]) * 1000)
                except Exception:
                    call_data["duration_ms"] = None
            else:
                call_data["duration_ms"] = None
            try:
                sa = call.get("started_at")
                ea = call.get("ended_at")
                call_data["start_timestamp"] = int(sa.timestamp() * 1000) if sa else None
                call_data["end_timestamp"] = int(ea.timestamp() * 1000) if ea else None
            except Exception:
                call_data["start_timestamp"] = None
                call_data["end_timestamp"] = None
            
            calls.append(call_data)

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


@router.get("/call-history/{call_id}")
async def get_call_history_item(call_id: str, user=Depends(get_current_user)):
    """
    History detail (per call id). Includes Retell webhook trail (events_log / agent_events).
    Shape is additive and compatible with reference-style fields.
    """
    row = db.get_call_dashboard_detail(call_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Call not found")

    # Reference-style aliases
    row["call_status"] = row.get("status")
    row["caller_phone"] = row.get("from_number")
    row["agent_phone"] = row.get("to_number")
    row["direction"] = "inbound"
    row["call_summary"] = row.get("summary")
    if row.get("duration") is not None:
        try:
            row["duration_ms"] = int(float(row["duration"]) * 1000)
        except Exception:
            row["duration_ms"] = None
    else:
        row["duration_ms"] = None
    try:
        sa = row.get("started_at")
        ea = row.get("ended_at")
        row["start_timestamp"] = int(sa.timestamp() * 1000) if sa else None
        row["end_timestamp"] = int(ea.timestamp() * 1000) if ea else None
    except Exception:
        row["start_timestamp"] = None
        row["end_timestamp"] = None

    # Human-friendly transcript text
    transcript_text = None
    tr = row.get("transcript")
    try:
        if isinstance(tr, str):
            tr = json.loads(tr)
        if isinstance(tr, list):
            lines = []
            for msg in tr:
                if isinstance(msg, dict) and msg.get("type") == "message":
                    speaker = "Assistant" if msg.get("role") == "assistant" else "User"
                    text = " ".join(msg.get("content", [])) if isinstance(msg.get("content"), list) else str(msg.get("content"))
                    lines.append(f"{speaker}: {text}")
            transcript_text = "\n".join(lines)
        elif isinstance(tr, dict):
            # retell transcript payload wrapper (we keep raw; text extraction best-effort)
            transcript_text = None
    except Exception:
        transcript_text = None
    row["transcript_text"] = transcript_text
    row["has_recording"] = bool(row.get("recording_url") or row.get("recording_blob_data"))

    return JSONResponse(content=jsonable_encoder(row))


@router.get("/agent/get-appointments/{user_id}")
async def get_appointments(user_id: int, from_date: str = None):
    """API for LiveKit agent to get all appointments for checking conflicts"""
    try:
        appointments = db.get_user_appointments(user_id, from_date)
        
        return JSONResponse({
            "success": True,
            "user_id": user_id,
            "appointments": [
                {
                    "id": apt["id"],
                    "date": str(apt["appointment_date"]),
                    "start_time": str(apt["start_time"]),
                    "end_time": str(apt["end_time"]),
                    "attendee_email": apt["attendee_email"],
                    "attendee_name": apt["attendee_name"],
                    "title": apt["title"],
                    "description": apt["description"],
                    "status": apt["status"]
                }
                for apt in appointments
            ]
        })
        
    except Exception as e:
        logging.error(f"Error fetching appointments: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )


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


@router.post("/agent/book-appointment")
async def book_appointment(request: Request):
    """
    API for LiveKit agent to book an appointment
    ✅ SIMPLIFIED: No conflict checking - just book immediately
    """
    try:
        data = await request.json()

        # Minimal logging for debugging tool payload issues (do not log transcript text).
        try:
            logging.info(
                "book-appointment: incoming keys=%s raw_user_id=%r",
                list(data.keys()) if isinstance(data, dict) else None,
                (data.get("user_id") if isinstance(data, dict) else None),
            )
        except Exception:
            pass

        # Always resolve to a valid business user id.
        # - Prefer a valid payload user_id (if present)
        # - Otherwise fall back to RETELL_DEFAULT_INBOUND_USER_ID
        user_id = resolve_agent_tool_user_id(data.get("user_id"))
        # Accept a few common alternate field names (some flows send date/time keys).
        appointment_date = data.get("appointment_date") or data.get("date") or data.get("reservation_date")
        start_time = data.get("start_time") or data.get("time") or data.get("appointment_time")
        end_time = data.get("end_time") or data.get("end")
        attendee_name = data.get("attendee_name", "Valued Customer")
        title = data.get("title", "Appointment")
        description = data.get("description", "")
        organizer_name = (data.get("organizer_name") or "").strip()
        organizer_email = data.get("organizer_email")

        logging.info(
            "book-appointment: resolved user_id=%r appointment_date=%r start_time=%r end_time=%r title=%r attendee_name=%r email_present=%s",
            user_id,
            appointment_date,
            start_time,
            end_time,
            title,
            attendee_name,
            bool(organizer_email),
        )

        if user_id is None:
            logging.error("book-appointment: could not resolve user_id. raw=%r env=%r", data.get("user_id"), os.getenv("RETELL_DEFAULT_INBOUND_USER_ID"))
            return error_response(
                "Could not resolve business user_id. Set RETELL_DEFAULT_INBOUND_USER_ID (users.id).",
                status_code=400,
            )

        missing: list[str] = []
        if not appointment_date:
            missing.append("appointment_date")
        if not start_time:
            missing.append("start_time")
        if missing:
            logging.error("book-appointment: missing required fields=%s payload_keys=%s", missing, list(data.keys()))
            return error_response(f"Missing required fields: {', '.join(missing)}", status_code=400)

        # Tool callers sometimes omit organizer_name; default safely for inbound dealership use.
        if not organizer_name:
            organizer_name = "Dealership"

        if not end_time:
            try:
                # tolerate "HH:MM:SS"
                st_raw = str(start_time).strip()
                fmt = "%H:%M:%S" if len(st_raw.split(":")) == 3 else "%H:%M"
                st = datetime.strptime(st_raw, fmt)
                end_time = (st + timedelta(hours=1)).strftime("%H:%M")
            except ValueError:
                logging.error("book-appointment: invalid start_time=%r payload=%r", start_time, data)
                return error_response("Invalid start_time; use HH:MM (24-hour)", status_code=400)

        # Validate user exists (prevents FK failures).
        u = None
        try:
            u = db.get_user_by_id(user_id)
        except Exception:
            u = None
        if not u:
            logging.error("book-appointment: user not found user_id=%r env_default=%r", user_id, os.getenv("RETELL_DEFAULT_INBOUND_USER_ID"))
            return error_response(
                f"Invalid business user mapping (user_id={user_id}). Ensure this users.id exists, or set RETELL_DEFAULT_INBOUND_USER_ID / RETELL_INBOUND_NUMBER_USER_MAP.",
                status_code=400,
            )

        # If caller didn't provide email, fall back to the business user's email.
        # This prevents production failures (appointments.attendee_email is NOT NULL).
        attendee_email = organizer_email
        if not attendee_email:
            try:
                attendee_email = (u.get("email") if isinstance(u, dict) else None) or "no-reply@example.com"
            except Exception:
                attendee_email = "no-reply@example.com"
        
        # ✅ REMOVED: Conflict checking
        # Just book directly
        
        appointment_id = db.create_appointment(
            user_id=user_id,
            appointment_date=appointment_date,
            start_time=start_time,
            end_time=end_time,
            attendee_name=attendee_name,
            attendee_email=attendee_email,
            title=title,
            description=description
        )
        
        # Send calendar invite email only if caller email was provided.
        email_sent = False
        if organizer_email:
            email_sent = await mail_obj.send_email_with_calendar_event(
                attendee_email=organizer_email,
                attendee_name=organizer_name,
                appointment_date=appointment_date,
                start_time=start_time,
                end_time=end_time,
                title=title,
                description=description,
                organizer_name=organizer_name,
                organizer_email=organizer_email
            )
        
        logging.info(f"✅ Appointment booked successfully: {appointment_id}")
        
        return JSONResponse({
            "success": True,
            "appointment_id": str(appointment_id),
            "email_sent": email_sent,
            "message": "Appointment booked successfully" + ("" if organizer_email else " (no email provided)")
        })
        
    except Exception as e:
        logging.error(f"❌ Error booking appointment: {e}")
        traceback.print_exc()
        return error_response(f"Failed to book appointment: {str(e)}", status_code=500)



@router.post("/agent/save-call-data")
async def save_call_data(request: Request):
    raise HTTPException(
        status_code=410,
        detail="save-call-data removed (Retell webhooks persist transcript/recording_url).",
    )
    



@router.post("/agent/report-event")
async def receive_agent_event(request: Request):
    try:
        data = await request.json()
        
        call_id = data.get("call_id")
        status = data.get("status")
        timestamp = data.get("timestamp")
        
        if not call_id or not status:
            return JSONResponse({"error": "Missing data"}, status_code=400)
        
        # Retell flow may report end outcomes; keep backward compatibility with LiveKit statuses.
        allowed = {
            "initialized",
            "dialing",
            "connected",
            "unanswered",
            "completed",
            "failed",
            "no_availability",
            "callback_requested",
        }
        if status not in allowed:
            return JSONResponse({"error": "Invalid status"}, status_code=400)
        
        # ✅ Build updates
        updates = {"status": status}
        now = datetime.now(timezone.utc)
        
        # ✅ Set started_at on dialing/connected
        if status in {"dialing", "connected"}:
            conn = db.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT started_at FROM call_history WHERE call_id = %s",
                        (call_id,)
                    )
                    row = cursor.fetchone()
                    if row and not row[0]:
                        updates["started_at"] = now
            finally:
                db.release_connection(conn)
        
        # ✅ Handle terminal states
        if status in {"unanswered", "completed", "failed", "no_availability", "callback_requested"}:
            updates["ended_at"] = now
            if status in {"unanswered", "failed"}:
                updates["duration"] = 0
        
        db.update_call_history(call_id, updates)
        
        # Retell tool expects acknowledged=true (see agent.json response_variables)
        return JSONResponse({"success": True, "acknowledged": True})
        
    except Exception as e:
        logging.error(f"report-event error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    








from pydantic import BaseModel, Field

class PromptCustomizationUpdate(BaseModel):
    system_prompt: str = Field(..., min_length=10, max_length=10000)


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

@router.options("/calls/{call_id}/recording/stream")
async def stream_call_recording_options(call_id: str):
    raise HTTPException(
        status_code=410,
        detail="Recording streaming removed. Use call_history.recording_url from /api/call-history.",
    )

@router.get("/calls/{call_id}/recording/stream")
async def stream_call_recording(
    call_id: str, 
    user=Depends(get_current_user),
    request: Request = None
):
    raise HTTPException(
        status_code=410,
        detail="Recording streaming removed. Use call_history.recording_url from /api/call-history.",
    )
    

@router.get("/calls/{call_id}/transcript")
async def get_call_transcript(call_id: str, user=Depends(get_current_user)):
    """Get transcript for a specific call"""
    try:
        conn = db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT transcript
                    FROM call_history
                    WHERE call_id = %s AND user_id = %s
                """, (call_id, user["id"]))
                row = cursor.fetchone()
        finally:
            db.release_connection(conn)
        
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Transcript not found")
        
        return JSONResponse({"transcript": row[0]})
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching transcript: {e}")
        raise HTTPException(status_code=500, detail=str(e))