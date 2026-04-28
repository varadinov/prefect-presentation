from __future__ import annotations

from datetime import date
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import FastAPI, Path
from pydantic import BaseModel, EmailStr, Field, computed_field

app = FastAPI()


class UserCreate(BaseModel):
    email: EmailStr
    name: Annotated[str, Field(min_length=1, max_length=60)]
    birthday: date


class UserOut(UserCreate):
    id: UUID

    @computed_field
    @property
    def age(self) -> int:
        today = date.today()
        years = today.year - self.birthday.year
        had_birthday = (today.month, today.day) >= (self.birthday.month, self.birthday.day)
        return years if had_birthday else years - 1


DB: dict[UUID, UserOut] = {}


@app.post("/users", response_model=UserOut)
def create_user(user: UserCreate) -> UserOut:
    user_out = UserOut(id=uuid4(), **user.model_dump())
    DB[user_out.id] = user_out
    return user_out


@app.get("/users/{user_id}", response_model=UserOut)
def get_user(
    user_id: Annotated[UUID, Path(description="User id (UUID)")],
) -> UserOut:
    return DB[user_id]
