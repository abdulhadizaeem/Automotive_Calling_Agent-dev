import os
import hashlib
import hmac

import httpx
from src.utils.db import Caller

RETELL_WEBHOOK_SECRET = os.getenv("RETELL_WEBHOOK_SECRET", "")
BASE_URL = "https://api.retellai.com"

LOCKED_PROMPT_TAIL = (
    "\n\nDYNAMIC VARIABLES AVAILABLE (do not edit this section):\n"
    "- {{customer_name}} — caller's saved name, or empty string if new\n"
    "- {{is_returning_customer}} — 'true' if caller has called before, 'false' if new\n"
    "- {{customer_phone}} — caller's phone number in E.164 format\n"
    "- {{kitchen_is_open}} — 'true' if kitchen is currently accepting orders, 'false' if not\n"
    "- {{store_is_open}} — 'true' if the restaurant is currently open, 'false' if closed\n"
    "- {{menu}} — the complete current restaurant menu with all available items, prices, "
    "allergens, and today's specials. This is your ONLY source of truth for menu items and "
    "pricing. Never reference items not listed in {{menu}}.\n"
    "- {{restaurant_info}} — general restaurant information."
)


def assemble_global_prompt(instructions: str) -> str:
    return instructions.rstrip() + LOCKED_PROMPT_TAIL



def _headers() -> dict:
    api_key = os.getenv("RETELL_API_KEY", "")
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


async def get_call(call_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/v2/get-call/{call_id}", headers=_headers())
        response.raise_for_status()
        return response.json()


async def get_agent() -> dict:
    agent_id = os.getenv("RETELL_AGENT_ID", "")
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/get-agent/{agent_id}",
            headers=_headers(),
        )
        response.raise_for_status()
        return response.json()


async def get_conversation_flow() -> dict:
    flow_id = os.getenv("RETELL_CONVERSATION_FLOW_ID", "")
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/get-conversation-flow/{flow_id}",
            headers=_headers(),
        )
        response.raise_for_status()
        return response.json()


async def update_conversation_flow(payload: dict) -> dict:
    flow_id = os.getenv("RETELL_CONVERSATION_FLOW_ID", "")
    patch_body: dict = {}
    if "global_prompt" in payload:
        patch_body["global_prompt"] = payload["global_prompt"]
    if "default_dynamic_variables" in payload:
        patch_body["default_dynamic_variables"] = payload["default_dynamic_variables"]
    if "model_choice" in payload:
        patch_body["model_choice"] = payload["model_choice"]
    if "model_temperature" in payload:
        patch_body["model_temperature"] = payload["model_temperature"]
    if "knowledge_base_ids" in payload:
        patch_body["knowledge_base_ids"] = payload["knowledge_base_ids"]
    if payload.get("begin_message") is not None:
        patch_body["begin_message"] = payload["begin_message"]
    if "nodes" in payload and payload["nodes"]:
        current = await get_conversation_flow()
        existing_nodes = {n["id"]: n for n in current.get("nodes", [])}
        for updated_node in payload["nodes"]:
            node_id = updated_node["id"]
            if node_id in existing_nodes and "instruction" in updated_node:
                existing_nodes[node_id]["instruction"] = updated_node["instruction"]
            elif node_id not in existing_nodes:
                existing_nodes[node_id] = updated_node
        patch_body["nodes"] = list(existing_nodes.values())
    async with httpx.AsyncClient() as client:
        response = await client.patch(
            f"{BASE_URL}/update-conversation-flow/{flow_id}",
            headers=_headers(),
            json=patch_body,
        )
        response.raise_for_status()
        return response.json()


async def update_agent_voice_settings(**kwargs) -> dict:
    agent_id = os.getenv("RETELL_AGENT_ID", "")
    payload = {k: v for k, v in kwargs.items() if v is not None}
    async with httpx.AsyncClient() as client:
        response = await client.patch(
            f"{BASE_URL}/update-agent/{agent_id}",
            headers=_headers(),
            json=payload,
        )
        response.raise_for_status()
        return response.json()



async def list_voices() -> list[dict]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/list-voices",
            headers=_headers(),
        )
        response.raise_for_status()
        return response.json()


async def list_knowledge_bases() -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/list-knowledge-bases", headers=_headers())
        response.raise_for_status()
        return response.json()


async def add_knowledge_base_to_flow(kb_id: str) -> dict:
    current = await get_conversation_flow()
    kb_ids = current.get("knowledge_base_ids", [])
    if kb_id not in kb_ids:
        kb_ids = kb_ids + [kb_id]
    return await update_conversation_flow({"knowledge_base_ids": kb_ids})


async def remove_knowledge_base_from_flow(kb_id: str) -> dict:
    current = await get_conversation_flow()
    kb_ids = [k for k in current.get("knowledge_base_ids", []) if k != kb_id]
    return await update_conversation_flow({"knowledge_base_ids": kb_ids})


def verify_webhook_signature(payload_bytes: bytes, signature_header: str, secret: str | None = None) -> bool:
    key = secret if secret is not None else RETELL_WEBHOOK_SECRET
    expected = hmac.new(
        key.encode(),
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


def build_caller_dynamic_variables(caller: Caller | None, from_number: str) -> dict:
    return {
        "customer_name": caller.customer_name if caller and caller.customer_name else "",
        "customer_phone": from_number,
        "is_returning_customer": "true" if caller else "false",
    }
