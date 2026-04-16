import os
import hashlib

import aiosmtplib
from email.message import EmailMessage
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.db import User
from src.utils.db_functions import (
    get_user_by_email,
    get_user_by_id,
    create_user,
    update_reset_token,
    clear_reset_token,
    update_password,
)
from src.utils.password_handler import hash_password, verify_password
from src.utils.jwt_handler import (
    create_access_token,
    create_refresh_token,
    decode_token,
    create_password_reset_token,
)
from fastapi import HTTPException, status
from sqlalchemy import select

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")


async def register(db: AsyncSession, email: str, password: str, full_name: str) -> User:
    existing = await get_user_by_email(db, email)
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    hashed = hash_password(password)
    return await create_user(db, email, hashed, full_name)


async def login(db: AsyncSession, email: str, password: str) -> dict:
    user = await get_user_by_email(db, email)
    if not user or not verify_password(password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is inactive")
    token_data = {"sub": user.id}
    return {
        "access_token": create_access_token(token_data),
        "refresh_token": create_refresh_token(token_data),
        "token_type": "bearer",
        "is_admin": user.is_admin,
    }


async def refresh_access_token(db: AsyncSession, refresh_token: str) -> dict:
    payload = decode_token(refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")
    user_id = payload.get("sub")
    user = await get_user_by_id(db, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return {
        "access_token": create_access_token({"sub": user.id}),
        "token_type": "bearer",
    }


async def forgot_password(db: AsyncSession, email: str) -> None:
    user = await get_user_by_email(db, email)
    if not user:
        return
    raw_token, expiry = create_password_reset_token()
    await update_reset_token(db, user.id, raw_token, expiry)
    reset_link = f"{FRONTEND_URL}/reset-password?token={raw_token}"
    message = EmailMessage()
    message["From"] = SMTP_USER
    message["To"] = user.email
    message["Subject"] = "Password Reset Request"
    message.set_content(f"Click the link to reset your password:\n\n{reset_link}\n\nThis link expires in 1 hour.")
    await aiosmtplib.send(
        message,
        hostname=SMTP_HOST,
        port=SMTP_PORT,
        username=SMTP_USER,
        password=SMTP_PASSWORD,
        start_tls=True,
    )


async def reset_password(db: AsyncSession, token: str, new_password: str) -> None:
    hashed_token = hashlib.sha256(token.encode()).hexdigest()
    result = await db.execute(
        select(User).where(
            User.reset_token == hashed_token,
            User.reset_token_expires > datetime.now(timezone.utc),
        )
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired reset token")
    hashed = hash_password(new_password)
    await update_password(db, user.id, hashed)
    await clear_reset_token(db, user.id)
