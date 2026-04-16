from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.auth.router import get_current_user
from src.utils.db import get_db, Prompt


router = APIRouter(prefix="/api/prompts", tags=["prompts"])


class PromptCreate(BaseModel):
    name: str
    description: str | None = None
    text: str


class PromptResponse(BaseModel):
    id: str
    name: str
    version: int
    description: str | None
    text: str
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


@router.get("", response_model=list[PromptResponse])
async def get_prompts(
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
):
    result = await db.execute(
        select(Prompt).order_by(Prompt.name, Prompt.version.desc())
    )
    return list(result.scalars().all())


@router.post("", response_model=PromptResponse, status_code=status.HTTP_201_CREATED)
async def create_prompt(
    body: PromptCreate,
    db: AsyncSession = Depends(get_db),
    _=Depends(get_current_user),
):
    latest_version = await db.scalar(
        select(func.max(Prompt.version)).where(Prompt.name == body.name)
    )
    new_version = (latest_version or 0) + 1

    prompt = Prompt(
        name=body.name,
        version=new_version,
        description=body.description,
        text=body.text,
        is_active=True,
        created_at=datetime.now(timezone.utc),
    )
    db.add(prompt)
    await db.commit()
    await db.refresh(prompt)
    return prompt
