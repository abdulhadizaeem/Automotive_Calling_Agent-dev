from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from src.utils.db import PGDB
from src.utils.utils import get_current_user

db = PGDB()

router = APIRouter(prefix="/prompts", tags=["prompts"])


class PromptCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    text: str = Field(..., min_length=1)


class PromptResponse(BaseModel):
    id: str
    name: str
    version: int
    description: str | None
    text: str
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


def _row_to_response(row: dict) -> PromptResponse:
    return PromptResponse(
        id=str(row["id"]),
        name=row["name"],
        version=int(row["version"]),
        description=row.get("description"),
        text=row["text"],
        is_active=bool(row.get("is_active", True)),
        created_at=row["created_at"],
    )


@router.get("", response_model=list[PromptResponse])
async def get_prompts(current_user=Depends(get_current_user)):
    rows = db.list_prompts_for_user(int(current_user["id"]))
    return [_row_to_response(r) for r in rows]


@router.post("", response_model=PromptResponse, status_code=status.HTTP_201_CREATED)
async def create_prompt(
    body: PromptCreate,
    current_user=Depends(get_current_user),
):
    name = body.name.strip()
    text = body.text.strip()
    if not name or not text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="name and text must be non-empty",
        )
    desc = body.description.strip() if body.description else None
    if desc == "":
        desc = None
    try:
        row = db.create_prompt_version(
            user_id=int(current_user["id"]),
            name=name,
            description=desc,
            text=text,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e
    return _row_to_response(row)
