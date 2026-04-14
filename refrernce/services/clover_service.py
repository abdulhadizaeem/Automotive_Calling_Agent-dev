import asyncio
import logging
import os

import httpx


_logger = logging.getLogger(__name__)


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('CLOVER_API_TOKEN', '')}",
        "Content-Type": "application/json",
    }


def _merchant_base() -> str:
    merchant_id = os.getenv("CLOVER_MERCHANT_ID", "")
    base = os.getenv("CLOVER_BASE_URL", "https://api.clover.com/v3")
    return f"{base}/merchants/{merchant_id}"


async def push_order_to_clover(
    order_items: list[dict],
    customer_name: str,
    order_type_id: str | None = None,
) -> dict:
    order_type_id = order_type_id or os.getenv("CLOVER_ORDER_TYPE_ID", "")
    tax_rate_id = os.getenv("CLOVER_TAX_RATE_ID", "")

    line_items = []

    for item in order_items:
        name = item.get("item", "Unknown Item")
        quantity = int(item.get("quantity", 1))
        price_cents = int(float(item.get("price", 0)) * 100)
        note = item.get("special_instructions", "")
        clover_item_id = item.get("clover_item_id")

        if clover_item_id:
            # Linked inventory item — Clover uses its own registered price (zero prices impossible)
            entry: dict = {"item": {"id": clover_item_id}, "taxRates": [{"id": tax_rate_id}]}
        else:
            # Fallback: custom item with name + price
            if quantity > 1:
                display_name = f"{quantity}x {name}"
                total_price = price_cents * quantity
            else:
                display_name = name
                total_price = price_cents
            entry = {"name": display_name, "price": total_price, "taxRates": [{"id": tax_rate_id}]}

        if note:
            entry["note"] = note

        for _ in range(quantity if clover_item_id else 1):
            line_items.append(entry)

    company_name = os.getenv("CLOVER_COMPANY_NAME", "Hot Chickz")
    caller_note = customer_name if customer_name else "Unknown Caller"

    payload: dict = {
        "orderCart": {
            "title": f"{company_name} {caller_note}",
            "state": "open",
            "lineItems": line_items,
        }
    }
    if order_type_id:
        payload["orderCart"]["orderType"] = {"id": order_type_id}

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{_merchant_base()}/atomic_order/orders",
            headers=_headers(),
            json=payload,
        )
        r.raise_for_status()
        data = r.json()
        order_id = data.get("id") or data.get("href", "").split("/")[-1]
        _logger.info("Clover order created: %s", order_id)

        if order_id:
            # Wait 1 second for Clover to fully commit all line items before printing
            await asyncio.sleep(1)
            try:
                print_resp = await client.post(
                    f"{_merchant_base()}/print_event",
                    headers=_headers(),
                    json={"orderRef": {"id": order_id}},
                )
                _logger.info("Print event: %s", print_resp.status_code)
            except Exception as exc:
                _logger.error("Print event failed for order %s: %s", order_id, exc)

        return {"clover_order_id": order_id}


async def get_clover_inventory() -> list[dict]:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{_merchant_base()}/items",
            headers=_headers(),
            params={"expand": "categories", "limit": 500},
        )
        r.raise_for_status()
        return r.json().get("elements", [])


async def get_clover_order_types() -> list[dict]:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{_merchant_base()}/order_types", headers=_headers())
        r.raise_for_status()
        return r.json().get("elements", [])


async def fetch_clover_item(item_id: str) -> dict:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{_merchant_base()}/items/{item_id}",
            headers=_headers(),
            params={"expand": "categories"},
        )
        r.raise_for_status()
        return r.json()


async def fetch_all_clover_items() -> list[dict]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{_merchant_base()}/items",
            headers=_headers(),
            params={"expand": "categories", "limit": 500},
        )
        r.raise_for_status()
        return r.json().get("elements", [])