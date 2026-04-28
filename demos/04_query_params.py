from __future__ import annotations

from typing import Annotated, TypedDict

from fastapi import FastAPI, Query

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


@app.get("/items", response_model=list[Item])
def list_items(
    name: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=60,
            description="Filter by name (case-insensitive substring match)",
        ),
    ] = None,
) -> list[Item]:
    if name is None:
        return ITEMS
    needle = name.lower()
    return [it for it in ITEMS if needle in it["name"].lower()]