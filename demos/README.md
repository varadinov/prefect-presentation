# Install `uv` (Linux, macOS, Windows)

Official docs: `https://docs.astral.sh/uv/`

## Linux / macOS

Install:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Restart your shell (or open a new terminal), then verify:

```bash
uv --version
```

## Windows (PowerShell)

Install:

```powershell
irm https://astral.sh/uv/install.ps1 | iex
```

Close and re-open PowerShell, then verify:

```powershell
uv --version
```

# Run demos

* Create a virtual environment and install dependencies:

```bash
cd demos
uv sync
```

* Run FastAPI demos (pick one):

```bash
uv run fastapi dev 01_hello_world.py
uv run fastapi dev 03_path_params.py
uv run fastapi dev 04_query_params.py
uv run fastapi dev 06_dependencies.py
uv run fastapi dev 07_validation_422.py
uv run fastapi dev 08_async_io.py
```
