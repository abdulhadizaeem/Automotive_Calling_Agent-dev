import os
import uuid
from datetime import datetime, timezone, date

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship
from sqlalchemy import String, Boolean, DateTime, Text, Integer, BigInteger, JSON, Date, Float, ForeignKey

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String, nullable=False)
    full_name: Mapped[str | None] = mapped_column(String, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    reset_token: Mapped[str | None] = mapped_column(String, nullable=True)
    reset_token_expires: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Caller(Base):
    __tablename__ = "callers"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    phone_number: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    customer_name: Mapped[str | None] = mapped_column(String, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_called_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class CallLog(Base):
    __tablename__ = "call_logs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    call_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    caller_phone: Mapped[str] = mapped_column(String, nullable=False)
    customer_name: Mapped[str | None] = mapped_column(String, nullable=True)
    call_status: Mapped[str] = mapped_column(String, default="registered")
    direction: Mapped[str] = mapped_column(String, default="inbound")
    recording_url: Mapped[str | None] = mapped_column(String, nullable=True)
    transcript: Mapped[str | None] = mapped_column(Text, nullable=True)
    call_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_booked: Mapped[bool] = mapped_column(Boolean, default=False)
    call_successful: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    user_sentiment: Mapped[str | None] = mapped_column(String, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    start_timestamp: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end_timestamp: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    raw_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    order_items: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_type: Mapped[str | None] = mapped_column(String, nullable=True)
    special_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    call_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    customer_name_extracted: Mapped[str | None] = mapped_column(String, nullable=True)
    reservation_date: Mapped[str | None] = mapped_column(String, nullable=True)
    party_size: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    order_details: Mapped["Order"] = relationship(
        "Order",
        primaryjoin="Order.call_id == CallLog.call_id",
        foreign_keys="[Order.call_id]",
        uselist=False,
        lazy="selectin"
    )


class Reservation(Base):
    __tablename__ = "reservations"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    reservation_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    caller_phone: Mapped[str] = mapped_column(String, nullable=False)
    customer_name: Mapped[str] = mapped_column(String, nullable=False)
    reservation_date: Mapped[date] = mapped_column(Date, nullable=False)
    reservation_time: Mapped[str] = mapped_column(String, nullable=False)
    party_size: Mapped[int] = mapped_column(Integer, nullable=False)
    special_requests: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String, default="confirmed")
    call_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class AgentSettings(Base):
    __tablename__ = "agent_settings"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    voice_id: Mapped[str] = mapped_column(String, default="11labs-Valentina")
    voice_speed: Mapped[float] = mapped_column(Float, default=1.0)
    voice_temperature: Mapped[float] = mapped_column(Float, default=0.7)
    interruption_sensitivity: Mapped[float] = mapped_column(Float, default=0.8)
    responsiveness: Mapped[float] = mapped_column(Float, default=1.0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    kitchen_open_time: Mapped[str] = mapped_column(String, default="11:00")
    kitchen_close_time: Mapped[str] = mapped_column(String, default="21:40")
    store_open_time: Mapped[str] = mapped_column(String, default="11:00")
    store_close_time: Mapped[str] = mapped_column(String, default="22:00")
    closed_greeting: Mapped[str] = mapped_column(String, default="We are currently closed. Please call back during business hours.")
    open_greeting: Mapped[str | None] = mapped_column(String, nullable=True)
    restaurant_timezone: Mapped[str] = mapped_column(String, default="America/New_York")
    prompt_instructions: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_address: Mapped[str | None] = mapped_column(String, nullable=True)
    pickup_address: Mapped[str | None] = mapped_column(String, nullable=True)
    restaurant_name: Mapped[str] = mapped_column(String, default="our restaurant")
    restaurant_info: Mapped[str] = mapped_column(Text, default="We are open daily from 11am to 10pm.")
    wait_time_pickup: Mapped[str] = mapped_column(String, default="15")
    wait_time_delivery: Mapped[str] = mapped_column(String, default="30")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    call_id: Mapped[str | None] = mapped_column(String, nullable=True)
    caller_phone: Mapped[str] = mapped_column(String, nullable=False)
    customer_name: Mapped[str] = mapped_column(String, nullable=False)
    order_items: Mapped[list] = mapped_column(JSON, nullable=False)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    delivery_address: Mapped[str | None] = mapped_column(String, nullable=True)
    total_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String, default="received")
    special_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    clover_order_id: Mapped[str | None] = mapped_column(String, nullable=True)
    clover_synced: Mapped[bool] = mapped_column(Boolean, default=False)
    clover_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class MenuCategory(Base):
    __tablename__ = "menu_categories"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_available: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class MenuItem(Base):
    __tablename__ = "menu_items"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    category_id: Mapped[str] = mapped_column(String, ForeignKey("menu_categories.id"), nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    is_available: Mapped[bool] = mapped_column(Boolean, default=True)
    allergens: Mapped[str | None] = mapped_column(String, nullable=True)
    prep_time_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    clover_item_id: Mapped[str | None] = mapped_column(String, unique=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class MenuSpecial(Base):
    __tablename__ = "menu_specials"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    discount_type: Mapped[str] = mapped_column(String, nullable=False)
    discount_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    applicable_items: Mapped[str | None] = mapped_column(String, nullable=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Prompt(Base):
    __tablename__ = "prompts"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class CloverItemMap(Base):
    __tablename__ = "clover_item_map"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    item_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    clover_item_id: Mapped[str] = mapped_column(String, nullable=False)
    clover_item_name: Mapped[str | None] = mapped_column(String, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

from sqlalchemy import text


async def _run_migration(sql: str) -> None:
    """Run a single DDL statement in its own transaction so failures are isolated."""
    try:
        async with engine.begin() as conn:
            await conn.execute(text(sql))
    except Exception:
        pass


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    migrations = [
        "ALTER TABLE agent_settings ADD COLUMN IF NOT EXISTS restaurant_info VARCHAR DEFAULT 'We are open daily from 11am to 10pm.'",
        "ALTER TABLE agent_settings ADD COLUMN IF NOT EXISTS wait_time_pickup VARCHAR DEFAULT '15'",
        "ALTER TABLE agent_settings ADD COLUMN IF NOT EXISTS wait_time_delivery VARCHAR DEFAULT '30'",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS clover_order_id VARCHAR",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS clover_synced BOOLEAN DEFAULT FALSE",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS clover_error TEXT",
        "ALTER TABLE menu_items ADD COLUMN IF NOT EXISTS clover_item_id VARCHAR",
    ]
    for sql in migrations:
        await _run_migration(sql)
