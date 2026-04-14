from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi import status as http_status
from pydantic import BaseModel
from sqlalchemy import func as sa_func, select as sa_select
from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.db import get_db, User, MenuCategory, MenuItem, MenuSpecial
from src.utils.dependencies import get_current_user
from src.utils.db_functions import (
    create_category,
    list_categories,
    get_category,
    update_category,
    delete_category,
    create_item,
    list_items,
    get_item,
    update_item,
    delete_item,
    create_special,
    list_specials,
    get_special,
    update_special,
    delete_special,
    build_menu_text,
)

router = APIRouter(prefix="/api/menu", tags=["menu"])

ALLOWED_DISCOUNT_TYPES = {"percentage", "fixed_amount", "free_item", "combo"}


class MenuItemResponse(BaseModel):
    id: str
    category_id: str
    name: str
    description: str | None
    price: float
    is_available: bool
    allergens: str | None
    prep_time_minutes: int | None
    sort_order: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class MenuCategoryResponse(BaseModel):
    id: str
    name: str
    description: str | None
    sort_order: int
    is_available: bool
    created_at: datetime

    class Config:
        from_attributes = True


class MenuCategoryWithItemsResponse(MenuCategoryResponse):
    items: list[MenuItemResponse] = []


class MenuSpecialResponse(BaseModel):
    id: str
    title: str
    description: str
    discount_type: str
    discount_value: float | None
    applicable_items: str | None
    valid_from: datetime | None
    valid_until: datetime | None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class MenuItemCreate(BaseModel):
    category_id: str
    name: str
    description: str | None = None
    price: float
    is_available: bool = True
    allergens: str | None = None
    prep_time_minutes: int | None = None
    sort_order: int = 0


class MenuItemUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    price: float | None = None
    is_available: bool | None = None
    allergens: str | None = None
    prep_time_minutes: int | None = None
    sort_order: int | None = None
    category_id: str | None = None


class MenuCategoryCreate(BaseModel):
    name: str
    description: str | None = None
    sort_order: int = 0


class MenuCategoryUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    sort_order: int | None = None
    is_available: bool | None = None


class MenuSpecialCreate(BaseModel):
    title: str
    description: str
    discount_type: str
    discount_value: float | None = None
    applicable_items: str | None = None
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    is_active: bool = True


class MenuSpecialUpdate(BaseModel):
    title: str | None = None
    description: str | None = None
    discount_type: str | None = None
    discount_value: float | None = None
    applicable_items: str | None = None
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    is_active: bool | None = None


class MenuPreviewResponse(BaseModel):
    menu_text: str
    category_count: int
    item_count: int
    active_specials_count: int
    unavailable_items_count: int


@router.get("/categories", response_model=list[MenuCategoryWithItemsResponse])
async def get_categories(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    categories = await list_categories(db)
    result = []
    for cat in categories:
        items = await list_items(db, category_id=cat.id)
        result.append(
            MenuCategoryWithItemsResponse(
                id=cat.id,
                name=cat.name,
                description=cat.description,
                sort_order=cat.sort_order,
                is_available=cat.is_available,
                created_at=cat.created_at,
                items=[MenuItemResponse.model_validate(i) for i in items],
            )
        )
    return result


@router.post("/categories", response_model=MenuCategoryResponse, status_code=http_status.HTTP_201_CREATED)
async def post_category(
    body: MenuCategoryCreate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    category = await create_category(db, body.name, body.description, body.sort_order)
    return MenuCategoryResponse.model_validate(category)


@router.patch("/categories/{category_id}", response_model=MenuCategoryResponse)
async def patch_category(
    category_id: str,
    body: MenuCategoryUpdate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    updates = body.model_dump(exclude_none=True)
    updated = await update_category(db, category_id, **updates)
    return MenuCategoryResponse.model_validate(updated)


@router.delete("/categories/{category_id}")
async def remove_category(
    category_id: str,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
) -> dict:
    category = await get_category(db, category_id)
    if not category:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Category not found")
    await delete_category(db, category_id)
    return {"message": "Category deleted"}


@router.get("/items", response_model=list[MenuItemResponse])
async def get_items(
    category_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    items = await list_items(db, category_id=category_id)
    return [MenuItemResponse.model_validate(i) for i in items]


@router.post("/items", response_model=MenuItemResponse, status_code=http_status.HTTP_201_CREATED)
async def post_item(
    body: MenuItemCreate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    item = await create_item(
        db,
        category_id=body.category_id,
        name=body.name,
        description=body.description,
        price=body.price,
        is_available=body.is_available,
        allergens=body.allergens,
        prep_time_minutes=body.prep_time_minutes,
        sort_order=body.sort_order,
    )
    return MenuItemResponse.model_validate(item)


@router.patch("/items/{item_id}", response_model=MenuItemResponse)
async def patch_item(
    item_id: str,
    body: MenuItemUpdate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    updates = body.model_dump(exclude_none=True)
    updated = await update_item(db, item_id, **updates)
    return MenuItemResponse.model_validate(updated)


@router.delete("/items/{item_id}")
async def remove_item(
    item_id: str,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
) -> dict:
    await delete_item(db, item_id)
    return {"message": "Item deleted"}


@router.get("/specials", response_model=list[MenuSpecialResponse])
async def get_specials(
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    specials = await list_specials(db, active_only=active_only)
    return [MenuSpecialResponse.model_validate(s) for s in specials]


@router.post("/specials", response_model=MenuSpecialResponse, status_code=http_status.HTTP_201_CREATED)
async def post_special(
    body: MenuSpecialCreate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    if body.discount_type not in ALLOWED_DISCOUNT_TYPES:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"discount_type must be one of: {', '.join(ALLOWED_DISCOUNT_TYPES)}",
        )
    special = await create_special(
        db,
        title=body.title,
        description=body.description,
        discount_type=body.discount_type,
        discount_value=body.discount_value,
        applicable_items=body.applicable_items,
        valid_from=body.valid_from,
        valid_until=body.valid_until,
        is_active=body.is_active,
    )
    return MenuSpecialResponse.model_validate(special)


@router.patch("/specials/{special_id}", response_model=MenuSpecialResponse)
async def patch_special(
    special_id: str,
    body: MenuSpecialUpdate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    updates = body.model_dump(exclude_none=True)
    if "discount_type" in updates and updates["discount_type"] not in ALLOWED_DISCOUNT_TYPES:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"discount_type must be one of: {', '.join(ALLOWED_DISCOUNT_TYPES)}",
        )
    updated = await update_special(db, special_id, **updates)
    return MenuSpecialResponse.model_validate(updated)


@router.delete("/specials/{special_id}")
async def remove_special(
    special_id: str,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
) -> dict:
    await delete_special(db, special_id)
    return {"message": "Special deleted"}


@router.get("/preview", response_model=MenuPreviewResponse)
async def menu_preview(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    menu_text = await build_menu_text(db)
    category_count = await db.scalar(
        sa_select(sa_func.count()).select_from(MenuCategory).where(MenuCategory.is_available == True)
    ) or 0
    item_count = await db.scalar(
        sa_select(sa_func.count()).select_from(MenuItem).where(MenuItem.is_available == True)
    ) or 0
    unavailable_items_count = await db.scalar(
        sa_select(sa_func.count()).select_from(MenuItem).where(MenuItem.is_available == False)
    ) or 0
    specials = await list_specials(db, active_only=True)
    return MenuPreviewResponse(
        menu_text=menu_text,
        category_count=category_count,
        item_count=item_count,
        active_specials_count=len(specials),
        unavailable_items_count=unavailable_items_count,
    )
