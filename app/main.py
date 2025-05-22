# app/main.py
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from .services.board import load_board, load_season_board

BASE = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE / "templates"))

app = FastAPI()
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return {"msg": "alive"}


@app.get("/weekly-board", response_class=HTMLResponse, include_in_schema=False)
async def draft_board(
    request: Request,
    season: int = Query(2024),
    week: int = Query(1),
):
    board_df = load_board(season, week)
    board = board_df.to_dict(orient="records")

    # HTMX sends HX-Request header. If present, render *partial* only
    if request.headers.get("hx-request") == "true":
        return templates.TemplateResponse(
            "_board_table.html",
            {"request": request, "board": board},
        )

    # Full page render
    return templates.TemplateResponse(
        "weekly_board.html",
        {"request": request, "board": board, "season": season, "week": week},
    )


@app.get("/season", tags=["draft"])
async def season_board(request: Request, season: int = 2024, teams: int = 12):
    """
    Season-long draft board (VOR vs replacement).
    """
    try:
        board = load_season_board(season, teams)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    context = dict(
        request=request,
        title=f"{season} Draft Board (Full Season)",
        board=board.to_dict(orient="records"),
        season=season,
        mode="season",
    )
    return templates.TemplateResponse("board.html", context)
