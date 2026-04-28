from typing import Annotated

from fastapi import Depends, FastAPI, Header

app = FastAPI()


def get_settings() -> dict[str, str]:
    return {"api_key": "demo-key"}


def get_request_id(
    x_request_id: Annotated[str | None, Header()] = None,
) -> str | None:
    return x_request_id


@app.get("/secure")
def secure(data: dict = Depends(get_settings)) -> dict[str, bool]:
    return {"configured": bool(data.get("api_key"))}


@app.get("/trace")
def trace(
    settings: dict = Depends(get_settings),
    request_id: str | None = Depends(get_request_id),
) -> dict[str, str | bool | None]:
    return {
        "configured": bool(settings.get("api_key")),
        "x_request_id": request_id,
    }
