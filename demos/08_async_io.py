import httpx
from fastapi import FastAPI

app = FastAPI()


@app.get("/posts/{post_id}")
async def read_remote_post(post_id: int) -> dict:
    """Await network I/O so other requests can run while this one waits."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"https://jsonplaceholder.typicode.com/posts/{post_id}",
        )
        response.raise_for_status()
        return response.json()
