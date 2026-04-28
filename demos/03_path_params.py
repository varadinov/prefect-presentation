from __future__ import annotations

from typing import Annotated, TypedDict

from fastapi import FastAPI, HTTPException, Path

app = FastAPI()

class Item(TypedDict):
    id: int
    name: str
    tags: list[str]
    price: float


ITEMS: list[Item] = [
    {"id": 1, "name": "Widget", "tags": ["hardware", "sale"], "price": 9.99},
    {"id": 2, "name": "Gadget", "tags": ["hardware"], "price": 19.99},
    {"id": 3, "name": "Book", "tags": ["books"], "price": 12.50},
    {"id": 4, "name": "Keyboard", "tags": ["hardware", "office"], "price": 49.00},
    {"id": 5, "name": "Notebook", "tags": ["office", "books"], "price": 4.25},
]


@app.get("/items/{item_id}", response_model=Item)
def get_item(
    item_id: Annotated[int, Path(ge=1, le=100, description="Item id (1..100)")],
) -> Item:
    matched = next(iter([it for it in ITEMS if it["id"] == item_id]), None)
    if not matched:
        raise HTTPException(status_code=404, detail="Item not found")
        
    return matched
