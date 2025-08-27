run: ; uvicorn src.app.main:app --reload
fmt: ; ruff format .
lint: ; ruff check --fix .
type: ; mypy src
test: ; pytest -q
