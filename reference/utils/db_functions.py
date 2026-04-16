import hashlib
import random
import secrets
import string
from datetime import datetime, timezone, date, timedelta

from fastapi import HTTPException
from fastapi import status as http_status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from sqlalchemy.orm import selectinload

from src.utils.db import (
    User, Caller, CallLog, Reservation, AgentSettings, Order,
    MenuCategory, MenuItem, MenuSpecial,
)


async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


async def get_user_by_id(db: AsyncSession, user_id: str) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def create_user(db: AsyncSession, email: str, hashed_password: str, full_name: str) -> User:
    user = User(email=email, hashed_password=hashed_password, full_name=full_name)
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def update_reset_token(db: AsyncSession, user_id: str, token: str, expires: datetime) -> None:
    hashed = hashlib.sha256(token.encode()).hexdigest()
    await db.execute(
        update(User)
        .where(User.id == user_id)
        .values(reset_token=hashed, reset_token_expires=expires)
    )
    await db.commit()


async def clear_reset_token(db: AsyncSession, user_id: str) -> None:
    await db.execute(
        update(User).where(User.id == user_id).values(reset_token=None, reset_token_expires=None)
    )
    await db.commit()


async def update_password(db: AsyncSession, user_id: str, hashed_password: str) -> None:
    await db.execute(
        update(User).where(User.id == user_id).values(hashed_password=hashed_password)
    )
    await db.commit()


async def get_caller_by_phone(db: AsyncSession, phone_number: str) -> Caller | None:
    result = await db.execute(select(Caller).where(Caller.phone_number == phone_number))
    return result.scalar_one_or_none()


async def upsert_caller(db: AsyncSession, phone_number: str, customer_name: str | None) -> Caller:
    caller = await get_caller_by_phone(db, phone_number)
    if caller is None:
        caller = Caller(phone_number=phone_number, customer_name=customer_name)
        db.add(caller)
    elif customer_name and not caller.customer_name:
        caller.customer_name = customer_name
    await db.commit()
    await db.refresh(caller)
    return caller


async def update_caller_last_called(db: AsyncSession, phone_number: str) -> None:
    await db.execute(
        update(Caller)
        .where(Caller.phone_number == phone_number)
        .values(last_called_at=datetime.now(timezone.utc))
    )
    await db.commit()


async def create_call_log(
    db: AsyncSession,
    call_id: str,
    caller_phone: str,
    customer_name: str | None,
    direction: str,
    call_status: str,
) -> CallLog:
    log = CallLog(
        call_id=call_id,
        caller_phone=caller_phone,
        customer_name=customer_name,
        direction=direction,
        call_status=call_status,
    )
    db.add(log)
    await db.commit()
    await db.refresh(log)
    return log


async def update_call_log(db: AsyncSession, call_id: str, **kwargs) -> None:
    await db.execute(
        update(CallLog).where(CallLog.call_id == call_id).values(**kwargs)
    )
    await db.commit()


async def get_call_log_by_call_id(db: AsyncSession, call_id: str) -> CallLog | None:
    result = await db.execute(select(CallLog).options(selectinload(CallLog.order_details)).where(CallLog.call_id == call_id))
    return result.scalar_one_or_none()


async def list_call_logs(
    db: AsyncSession,
    skip: int,
    limit: int,
    status_filter: str | None,
    order_booked_filter: bool | None,
) -> list[CallLog]:
    query = select(CallLog).options(selectinload(CallLog.order_details)).order_by(CallLog.created_at.desc()).offset(skip).limit(limit)
    if status_filter is not None:
        query = query.where(CallLog.call_status == status_filter)
    if order_booked_filter is not None:
        query = query.where(CallLog.order_booked == order_booked_filter)
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_combined_stats(db: AsyncSession) -> dict:
    total_calls = await db.scalar(select(func.count()).select_from(CallLog)) or 0
    successful_calls = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.call_successful == True)) or 0
    failed_calls = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.call_successful == False)) or 0
    pending_calls = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.call_successful == None)) or 0
    orders_booked = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.order_booked == True)) or 0
    order_calls = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.call_reason == "order_placed")) or 0
    orders_failed = max(order_calls - orders_booked, 0)
    total_res = await db.scalar(select(func.count()).select_from(Reservation)) or 0
    confirmed_res = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "confirmed")) or 0
    completed_res = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "completed")) or 0
    cancelled_res = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "cancelled")) or 0
    no_show_res = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "no_show")) or 0
    today_res = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.reservation_date == date.today())) or 0
    order_stats = await get_order_stats(db)
    return {
        "calls": {
            "total": total_calls,
            "successful": successful_calls,
            "failed": failed_calls,
            "pending": pending_calls,
        },
        "reservations": {
            "total": total_res,
            "confirmed": confirmed_res,
            "completed": completed_res,
            "cancelled": cancelled_res,
            "no_show": no_show_res,
            "today": today_res,
        },
        "orders": order_stats,
    }


async def create_reservation(
    db: AsyncSession,
    caller_phone: str,
    customer_name: str,
    reservation_date: date,
    reservation_time: str,
    party_size: int,
    special_requests: str | None,
    call_id: str | None,
) -> Reservation:
    date_str = reservation_date.strftime("%Y%m%d")
    suffix = "".join(random.choices(string.ascii_uppercase, k=4))
    reservation_id = f"RES-{date_str}-{suffix}"
    reservation = Reservation(
        reservation_id=reservation_id,
        caller_phone=caller_phone,
        customer_name=customer_name,
        reservation_date=reservation_date,
        reservation_time=reservation_time,
        party_size=party_size,
        special_requests=special_requests or None,
        status="confirmed",
        call_id=call_id,
    )
    db.add(reservation)
    await db.commit()
    await db.refresh(reservation)
    return reservation


async def get_reservation_by_id(db: AsyncSession, reservation_id: str) -> Reservation | None:
    result = await db.execute(select(Reservation).where(Reservation.reservation_id == reservation_id))
    return result.scalar_one_or_none()


async def list_reservations(
    db: AsyncSession,
    skip: int,
    limit: int,
    date_filter: str | None,
    status_filter: str | None,
) -> list[Reservation]:
    query = (
        select(Reservation)
        .order_by(Reservation.reservation_date.asc(), Reservation.reservation_time.asc())
        .offset(skip)
        .limit(limit)
    )
    if date_filter:
        query = query.where(Reservation.reservation_date == date_filter)
    if status_filter:
        query = query.where(Reservation.status == status_filter)
    result = await db.execute(query)
    return list(result.scalars().all())


async def update_reservation(db: AsyncSession, reservation_id: str, **kwargs) -> Reservation:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    await db.execute(
        update(Reservation).where(Reservation.reservation_id == reservation_id).values(**kwargs)
    )
    await db.commit()
    return await get_reservation_by_id(db, reservation_id)


async def get_reservation_stats(db: AsyncSession) -> dict:
    total = await db.scalar(select(func.count()).select_from(Reservation)) or 0
    confirmed = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "confirmed")) or 0
    completed = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "completed")) or 0
    cancelled = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "cancelled")) or 0
    no_show = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.status == "no_show")) or 0
    today_count = await db.scalar(select(func.count()).select_from(Reservation).where(Reservation.reservation_date == date.today())) or 0
    return {
        "total": total,
        "confirmed": confirmed,
        "completed": completed,
        "cancelled": cancelled,
        "no_show": no_show,
        "today": today_count,
    }


async def link_reservation_to_call(db: AsyncSession, caller_phone: str, call_id: str) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)
    result = await db.execute(
        select(Reservation)
        .where(
            Reservation.caller_phone == caller_phone,
            Reservation.call_id == None,
            Reservation.created_at >= cutoff,
        )
        .order_by(Reservation.created_at.desc())
        .limit(1)
    )
    reservation = result.scalar_one_or_none()
    if reservation:
        await db.execute(
            update(Reservation)
            .where(Reservation.id == reservation.id)
            .values(call_id=call_id, updated_at=datetime.now(timezone.utc))
        )
        await db.commit()


async def get_agent_settings(db: AsyncSession) -> AgentSettings:
    result = await db.execute(select(AgentSettings).limit(1))
    settings = result.scalar_one_or_none()
    if settings is None:
        settings = AgentSettings()
        db.add(settings)
        await db.commit()
        await db.refresh(settings)
    return settings


async def update_agent_settings(db: AsyncSession, **kwargs) -> AgentSettings:
    settings = await get_agent_settings(db)
    for key, value in kwargs.items():
        setattr(settings, key, value)
    settings.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(settings)
    return settings


async def create_category(
    db: AsyncSession, name: str, description: str | None, sort_order: int
) -> MenuCategory:
    category = MenuCategory(name=name, description=description, sort_order=sort_order)
    db.add(category)
    await db.commit()
    await db.refresh(category)
    return category


async def list_categories(db: AsyncSession) -> list[MenuCategory]:
    result = await db.execute(select(MenuCategory).order_by(MenuCategory.sort_order.asc()))
    return list(result.scalars().all())


async def get_category(db: AsyncSession, category_id: str) -> MenuCategory | None:
    result = await db.execute(select(MenuCategory).where(MenuCategory.id == category_id))
    return result.scalar_one_or_none()


async def update_category(db: AsyncSession, category_id: str, **kwargs) -> MenuCategory:
    category = await get_category(db, category_id)
    if not category:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Category not found")
    await db.execute(update(MenuCategory).where(MenuCategory.id == category_id).values(**kwargs))
    await db.commit()
    return await get_category(db, category_id)


async def delete_category(db: AsyncSession, category_id: str) -> None:
    items = await list_items(db, category_id=category_id)
    if items:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="Remove all items from this category before deleting it.",
        )
    await db.execute(delete(MenuCategory).where(MenuCategory.id == category_id))
    await db.commit()


async def create_item(
    db: AsyncSession,
    category_id: str,
    name: str,
    description: str | None,
    price: float,
    is_available: bool,
    allergens: str | None,
    prep_time_minutes: int | None,
    sort_order: int,
) -> MenuItem:
    category = await get_category(db, category_id)
    if not category:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Category not found")
    item = MenuItem(
        category_id=category_id,
        name=name,
        description=description,
        price=price,
        is_available=is_available,
        allergens=allergens,
        prep_time_minutes=prep_time_minutes,
        sort_order=sort_order,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


async def list_items(db: AsyncSession, category_id: str | None = None) -> list[MenuItem]:
    query = select(MenuItem).order_by(MenuItem.sort_order.asc())
    if category_id is not None:
        query = query.where(MenuItem.category_id == category_id)
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_item(db: AsyncSession, item_id: str) -> MenuItem | None:
    result = await db.execute(select(MenuItem).where(MenuItem.id == item_id))
    return result.scalar_one_or_none()


async def update_item(db: AsyncSession, item_id: str, **kwargs) -> MenuItem:
    item = await get_item(db, item_id)
    if not item:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Item not found")
    kwargs["updated_at"] = datetime.now(timezone.utc)
    await db.execute(update(MenuItem).where(MenuItem.id == item_id).values(**kwargs))
    await db.commit()
    return await get_item(db, item_id)


async def delete_item(db: AsyncSession, item_id: str) -> None:
    item = await get_item(db, item_id)
    if not item:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Item not found")
    await db.execute(delete(MenuItem).where(MenuItem.id == item_id))
    await db.commit()


async def create_special(
    db: AsyncSession,
    title: str,
    description: str,
    discount_type: str,
    discount_value: float | None,
    applicable_items: str | None,
    valid_from: datetime | None,
    valid_until: datetime | None,
    is_active: bool,
) -> MenuSpecial:
    special = MenuSpecial(
        title=title,
        description=description,
        discount_type=discount_type,
        discount_value=discount_value,
        applicable_items=applicable_items,
        valid_from=valid_from,
        valid_until=valid_until,
        is_active=is_active,
    )
    db.add(special)
    await db.commit()
    await db.refresh(special)
    return special


async def list_specials(db: AsyncSession, active_only: bool = False) -> list[MenuSpecial]:
    query = select(MenuSpecial)
    if active_only:
        now = datetime.now(timezone.utc)
        query = query.where(
            MenuSpecial.is_active == True,
            (MenuSpecial.valid_from == None) | (MenuSpecial.valid_from <= now),
            (MenuSpecial.valid_until == None) | (MenuSpecial.valid_until >= now),
        )
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_special(db: AsyncSession, special_id: str) -> MenuSpecial | None:
    result = await db.execute(select(MenuSpecial).where(MenuSpecial.id == special_id))
    return result.scalar_one_or_none()


async def update_special(db: AsyncSession, special_id: str, **kwargs) -> MenuSpecial:
    special = await get_special(db, special_id)
    if not special:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Special not found")
    kwargs["updated_at"] = datetime.now(timezone.utc)
    await db.execute(update(MenuSpecial).where(MenuSpecial.id == special_id).values(**kwargs))
    await db.commit()
    return await get_special(db, special_id)


async def delete_special(db: AsyncSession, special_id: str) -> None:
    special = await get_special(db, special_id)
    if not special:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Special not found")
    await db.execute(delete(MenuSpecial).where(MenuSpecial.id == special_id))
    await db.commit()


async def build_menu_text(db: AsyncSession) -> str:
    categories = await list_categories(db)
    available_categories = [c for c in categories if c.is_available]
    if not available_categories:
        return "Menu currently unavailable."
    lines = ["RESTAURANT MENU"]
    has_any_item = False
    for category in available_categories:
        items = await list_items(db, category_id=category.id)
        available_items = [i for i in items if i.is_available]
        if not available_items:
            continue
        has_any_item = True
        lines.append(f"\n=== {category.name} ===")
        for item in available_items:
            line = f"- {item.name} — ${item.price:.2f}"
            if item.description:
                line += f" | {item.description}"
            if item.allergens:
                line += f" | Allergens: {item.allergens}"
            if item.prep_time_minutes:
                line += f" | Prep: {item.prep_time_minutes} min"
            lines.append(line)
    if not has_any_item:
        return "Menu currently unavailable."
    specials = await list_specials(db, active_only=True)
    if specials:
        lines.append("\nTODAY'S SPECIALS")
        for special in specials:
            line = f"- {special.title}: {special.description}"
            if special.discount_type == "percentage" and special.discount_value:
                target = f" on {special.applicable_items}" if special.applicable_items else ""
                line += f" ({int(special.discount_value)}% off{target})"
            elif special.discount_type == "fixed_amount" and special.discount_value:
                line += f" (${special.discount_value:.2f} off)"
            if special.valid_until:
                line += f", valid until {special.valid_until.strftime('%I:%M%p').lstrip('0')}"
            lines.append(line)
    return "\n".join(lines)


async def create_order(
    db: AsyncSession,
    caller_phone: str,
    customer_name: str,
    order_items: list,
    order_type: str,
    delivery_address: str | None,
    total_amount: float | None,
    special_notes: str | None,
    call_id: str | None = None,
) -> Order:
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    suffix = "".join(secrets.choice(string.ascii_uppercase) for _ in range(4))
    order_id = f"ORD-{date_str}-{suffix}"
    order = Order(
        order_id=order_id,
        call_id=call_id,
        caller_phone=caller_phone,
        customer_name=customer_name,
        order_items=order_items,
        order_type=order_type,
        delivery_address=delivery_address or None,
        total_amount=total_amount,
        status="received",
        special_notes=special_notes or None,
    )
    db.add(order)
    await db.commit()
    await db.refresh(order)
    return order


async def list_orders(
    db: AsyncSession,
    skip: int = 0,
    limit: int = 20,
    status_filter: str | None = None,
    date_filter: str | None = None,
) -> list[Order]:
    query = select(Order).order_by(Order.created_at.desc()).offset(skip).limit(limit)
    if status_filter:
        query = query.where(Order.status == status_filter)
    if date_filter:
        parsed = date.fromisoformat(date_filter)
        query = query.where(func.date(Order.created_at) == parsed)
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_order_by_id(db: AsyncSession, order_id: str) -> Order | None:
    result = await db.execute(select(Order).where(Order.order_id == order_id))
    return result.scalar_one_or_none()


async def update_order(db: AsyncSession, order_id: str, **kwargs) -> Order:
    order = await get_order_by_id(db, order_id)
    if not order:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Order not found")
    kwargs["updated_at"] = datetime.now(timezone.utc)
    await db.execute(update(Order).where(Order.order_id == order_id).values(**kwargs))
    await db.commit()
    return await get_order_by_id(db, order_id)


async def get_order_stats(db: AsyncSession) -> dict:
    total = await db.scalar(select(func.count()).select_from(Order)) or 0
    received = await db.scalar(select(func.count()).select_from(Order).where(Order.status == "received")) or 0
    preparing = await db.scalar(select(func.count()).select_from(Order).where(Order.status == "preparing")) or 0
    ready = await db.scalar(select(func.count()).select_from(Order).where(Order.status == "ready")) or 0
    completed = await db.scalar(select(func.count()).select_from(Order).where(Order.status == "completed")) or 0
    cancelled = await db.scalar(select(func.count()).select_from(Order).where(Order.status == "cancelled")) or 0
    today_count = await db.scalar(
        select(func.count()).select_from(Order).where(func.date(Order.created_at) == date.today())
    ) or 0
    return {
        "total": total,
        "received": received,
        "preparing": preparing,
        "ready": ready,
        "completed": completed,
        "cancelled": cancelled,
        "today": today_count,
    }


async def get_recent_order_for_caller(
    db: AsyncSession, caller_phone: str, minutes: int = 60
) -> Order | None:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    result = await db.execute(
        select(Order)
        .where(
            Order.caller_phone == caller_phone,
            Order.call_id == None,
            Order.created_at >= cutoff,
        )
        .order_by(Order.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_dashboard_stats(db: AsyncSession, days: int = 7) -> dict:
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import func, select
    from src.utils.db import CallLog, Order, Caller
    since = datetime.now(timezone.utc) - timedelta(days=days)
    total_calls = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.created_at >= since)) or 0
    total_orders = await db.scalar(select(func.count()).select_from(Order).where(Order.created_at >= since)) or 0
    total_minutes_ms = await db.scalar(select(func.sum(CallLog.duration_ms)).select_from(CallLog).where(CallLog.created_at >= since).where(CallLog.duration_ms.isnot(None))) or 0
    total_minutes = round((total_minutes_ms or 0) / 60000, 1)
    successful_calls = await db.scalar(select(func.count()).select_from(CallLog).where(CallLog.created_at >= since).where(CallLog.call_successful == True)) or 0
    subq = (
        select(CallLog.caller_phone)
        .where(CallLog.created_at >= since)
        .where(CallLog.caller_phone != "")
        .group_by(CallLog.caller_phone)
        .having(func.count() > 1)
        .subquery("sq")
    )
    repeat_callers = await db.scalar(select(func.count()).select_from(subq)) or 0
    new_callers = await db.scalar(select(func.count()).select_from(Caller).where(Caller.created_at >= since)) or 0
    pickup_orders = await db.scalar(select(func.count()).select_from(Order).where(Order.created_at >= since).where(Order.order_type == "pickup")) or 0
    delivery_orders = await db.scalar(select(func.count()).select_from(Order).where(Order.created_at >= since).where(Order.order_type == "delivery")) or 0
    dine_in_orders = await db.scalar(select(func.count()).select_from(Order).where(Order.created_at >= since).where(Order.order_type == "dine_in")) or 0
    return {
        "total_calls": total_calls,
        "total_orders": total_orders,
        "total_minutes": total_minutes,
        "successful_calls": successful_calls,
        "repeat_callers": repeat_callers,
        "new_callers": new_callers,
        "order_type_distribution": {
            "pickup": pickup_orders,
            "delivery": delivery_orders,
            "dine_in": dine_in_orders,
        }
    }


async def get_calls_over_time(db: AsyncSession, days: int = 7) -> list:
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import func, select, cast, Date
    from src.utils.db import CallLog
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(cast(CallLog.created_at, Date).label("day"), func.count().label("calls"))
        .where(CallLog.created_at >= since)
        .group_by(cast(CallLog.created_at, Date))
        .order_by(cast(CallLog.created_at, Date))
    )
    rows = result.all()
    date_map = {str(r.day): r.calls for r in rows}
    result_list = []
    for i in range(days):
        d = (datetime.now(timezone.utc) - timedelta(days=days - 1 - i)).date()
        result_list.append({"date": str(d), "calls": date_map.get(str(d), 0)})
    return result_list


async def get_orders_over_time(db: AsyncSession, days: int = 7) -> list:
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import func, select, cast, Date
    from src.utils.db import Order
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(cast(Order.created_at, Date).label("day"), func.count().label("orders"))
        .where(Order.created_at >= since)
        .group_by(cast(Order.created_at, Date))
        .order_by(cast(Order.created_at, Date))
    )
    rows = result.all()
    date_map = {str(r.day): r.orders for r in rows}
    result_list = []
    for i in range(days):
        d = (datetime.now(timezone.utc) - timedelta(days=days - 1 - i)).date()
        result_list.append({"date": str(d), "orders": date_map.get(str(d), 0)})
    return result_list


async def get_top_repeat_callers(db: AsyncSession, days: int = 7, limit: int = 10) -> list:
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import func, select
    from src.utils.db import CallLog
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(CallLog.caller_phone, CallLog.customer_name, func.count().label("call_count"))
        .where(CallLog.created_at >= since)
        .where(CallLog.caller_phone != "")
        .group_by(CallLog.caller_phone, CallLog.customer_name)
        .having(func.count() > 1)
        .order_by(func.count().desc())
        .limit(limit)
    )
    rows = result.all()
    return [
        {"phone": r.caller_phone, "name": r.customer_name or "Unknown", "call_count": r.call_count}
        for r in rows
    ]


async def get_sentiment_breakdown(db: AsyncSession, days: int = 7) -> dict:
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import func, select
    from src.utils.db import CallLog
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(CallLog.user_sentiment, func.count().label("count"))
        .where(CallLog.created_at >= since)
        .where(CallLog.user_sentiment.isnot(None))
        .group_by(CallLog.user_sentiment)
    )
    rows = result.all()
    return {r.user_sentiment: r.count for r in rows}


async def get_clover_item_map(db: AsyncSession) -> dict[str, str]:
    from src.utils.db import CloverItemMap
    result = await db.execute(
        select(CloverItemMap).where(CloverItemMap.is_active == True)
    )
    return {item.item_name: item.clover_item_id for item in result.scalars().all()}


async def upsert_clover_item(
    db: AsyncSession,
    item_name: str,
    clover_item_id: str,
    clover_item_name: str | None = None,
) -> None:
    from src.utils.db import CloverItemMap
    result = await db.execute(
        select(CloverItemMap).where(CloverItemMap.item_name == item_name)
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.clover_item_id = clover_item_id
        if clover_item_name:
            existing.clover_item_name = clover_item_name
        existing.updated_at = datetime.now(timezone.utc)
    else:
        db.add(CloverItemMap(
            item_name=item_name,
            clover_item_id=clover_item_id,
            clover_item_name=clover_item_name,
        ))
    await db.commit()


async def delete_clover_item(db: AsyncSession, item_name: str) -> None:
    from src.utils.db import CloverItemMap
    result = await db.execute(
        select(CloverItemMap).where(CloverItemMap.item_name == item_name)
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.is_active = False
        existing.updated_at = datetime.now(timezone.utc)
        await db.commit()


async def update_order_clover_status(
    db: AsyncSession,
    order_id: str,
    clover_order_id: str | None,
    synced: bool,
    error: str | None = None,
) -> None:
    from src.utils.db import Order
    await db.execute(
        update(Order)
        .where(Order.order_id == order_id)
        .values(clover_order_id=clover_order_id, clover_synced=synced, clover_error=error)
    )
    await db.commit()



async def get_menu_items_prices(db: AsyncSession, item_names: list[str]) -> dict[str, float]:
    """Helper to fetch prices for multiple items by name."""
    if not item_names:
        return {}
    from src.utils.db import MenuItem
    result = await db.execute(
        select(MenuItem.name, MenuItem.price)
        .where(MenuItem.name.in_(item_names))
    )
    return {row[0].lower(): row[1] for row in result.all()}


async def get_menu_items_clover_data(db: AsyncSession, item_names: list[str]) -> dict[str, dict]:
    """
    Return map of original Retell item name -> {price, clover_item_id}.

    Matching strategy (in order):
    1. Exact case-insensitive match
    2. Unique prefix match — only if exactly ONE DB item starts with the search name
       (safe for "Hot Chickz #7" matching "Hot Chickz #7 BOWL ..." uniquely)
    """
    if not item_names:
        return {}
    from src.utils.db import MenuItem
    result = await db.execute(
        select(MenuItem.name, MenuItem.price, MenuItem.clover_item_id)
        .where(MenuItem.is_available == True)
    )
    all_rows = result.all()
    # Build a lookup keyed by lowercase DB name
    db_map = {row[0].lower(): {"price": row[1], "clover_item_id": row[2]} for row in all_rows}
    db_lower_names = list(db_map.keys())

    output: dict[str, dict] = {}

    SPICE_LEVELS = {"no spice", "mild", "hot", "blaze", "reaper"}

    for search_name in item_names:
        key = search_name.lower().strip()

        # Strip trailing spice level so "Hot Chickz #6 Reaper" -> "hot chickz #6"
        for spice in SPICE_LEVELS:
            if key.endswith(f" {spice}"):
                key = key[: -(len(spice) + 1)].strip()
                break

        # Pass 1: exact match
        if key in db_map:
            output[search_name] = db_map[key]
            continue

        # Pass 2: unique prefix match — safe only when exactly ONE DB item starts with key
        candidates = [db_name for db_name in db_lower_names if db_name.startswith(key)]
        if len(candidates) == 1:
            output[search_name] = db_map[candidates[0]]

    return output


CLOVER_ITEM_SKIP_PREFIXES = ("print", "gift card", "fb redeem", "waiver", "2oz", "cheese on fries")


def _is_clover_internal_item(item: dict) -> bool:
    """Return True if a Clover item should be excluded from the customer menu."""
    price_cents = item.get("price", 0)
    name = item.get("name", "").strip().lower()
    if price_cents == 0:
        return True
    if any(name.startswith(prefix) for prefix in CLOVER_ITEM_SKIP_PREFIXES):
        return True
    return False


async def upsert_menu_item_from_clover(db: AsyncSession, item: dict) -> bool:
    """Upsert a single Clover item into the local menu_items table.

    Returns True if the item was synced, False if it was skipped.
    Resolves the category by name from Clover's categories list or falls back
    to a default 'Clover Items' category so every item always has a home.
    """
    from src.utils.db import MenuItem, MenuCategory

    if _is_clover_internal_item(item):
        return False

    clover_id = item.get("id")
    name = item.get("name", "").strip()
    price_cents = item.get("price", 0)
    price = price_cents / 100.0
    hidden = item.get("hidden", False)

    cat_elements = item.get("categories", {}).get("elements", [])
    category_name = cat_elements[0].get("name", "Clover Items") if cat_elements else "Clover Items"

    cat_result = await db.execute(
        select(MenuCategory).where(MenuCategory.name == category_name)
    )
    category = cat_result.scalar_one_or_none()
    if not category:
        category = MenuCategory(name=category_name, is_available=True)
        db.add(category)
        await db.flush()

    item_result = await db.execute(
        select(MenuItem).where(MenuItem.clover_item_id == clover_id)
    )
    existing = item_result.scalar_one_or_none()

    now = datetime.now(timezone.utc)
    if existing:
        existing.name = name
        existing.price = price
        existing.category_id = category.id
        existing.is_available = not hidden
        existing.updated_at = now
    else:
        db.add(MenuItem(
            category_id=category.id,
            name=name,
            price=price,
            is_available=not hidden,
            clover_item_id=clover_id,
            created_at=now,
            updated_at=now,
        ))

    await db.commit()
    return True


async def full_sync_menu_from_clover(db: AsyncSession, clover_items: list[dict]) -> dict:
    """Full replacement sync: wipe items/categories not linked to Clover, then upsert.

    This prevents duplicate categories from accumulating when mixing
    manually-seeded data with Clover-synced data.
    """
    from src.utils.db import MenuItem, MenuCategory
    from sqlalchemy import delete as sa_delete

    await db.execute(
        sa_delete(MenuItem).where(MenuItem.clover_item_id.is_(None))
    )
    await db.commit()

    await db.execute(
        update(MenuItem)
        .where(MenuItem.clover_item_id.is_not(None))
        .values(is_available=False, updated_at=datetime.now(timezone.utc))
    )
    await db.commit()

    synced = 0
    skipped = 0
    for item in clover_items:
        result = await upsert_menu_item_from_clover(db, item)
        if result:
            synced += 1
        else:
            skipped += 1

    all_cats = (await db.execute(select(MenuCategory))).scalars().all()
    for cat in all_cats:
        available_count = await db.scalar(
            select(func.count()).select_from(MenuItem).where(
                MenuItem.category_id == cat.id,
                MenuItem.is_available == True,
            )
        )
        if not available_count:
            await db.execute(
                delete(MenuItem).where(MenuItem.category_id == cat.id)
            )
            await db.flush()
            await db.delete(cat)
    await db.commit()

    return {"synced": synced, "skipped": skipped}


async def delete_menu_item_by_clover_id(db: AsyncSession, clover_item_id: str) -> None:
    """Soft-delete a menu item when Clover sends a DELETE webhook event."""
    from src.utils.db import MenuItem
    await db.execute(
        update(MenuItem)
        .where(MenuItem.clover_item_id == clover_item_id)
        .values(is_available=False, updated_at=datetime.now(timezone.utc))
    )
    await db.commit()


