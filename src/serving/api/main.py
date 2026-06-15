"""FastAPI serving layer: station lookups (PostGIS) + hotspot/H3 aggregates (gold Delta)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from functools import partial

from fastapi import FastAPI

from serving.api.db import (
    APIConfig,
    create_connection_pool,
    gold_hotspots_available,
    read_gold_hotspots,
)
from serving.api.models import HealthOut
from serving.api.routers import h3, hotspots, stations


def create_app(config: APIConfig | None = None) -> FastAPI:
    config = config or APIConfig.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Postgres may not be up yet (or at all, for gold-only endpoints in
        # tests) -- /stations/nearby and /health degrade gracefully if so.
        try:
            app.state.pool = create_connection_pool(config)
        except Exception:
            app.state.pool = None

        yield

        if app.state.pool is not None:
            app.state.pool.closeall()

    app = FastAPI(title="NYC Mobility Lakehouse API", lifespan=lifespan)
    app.state.config = config
    app.state.pool = None
    app.state.gold_hotspots_reader = partial(read_gold_hotspots, config.gold_hotspots_path)

    app.include_router(stations.router)
    app.include_router(hotspots.router)
    app.include_router(h3.router)

    @app.get("/health", response_model=HealthOut)
    def health() -> HealthOut:
        postgres_ok = False
        if app.state.pool is not None:
            try:
                conn = app.state.pool.getconn()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                    postgres_ok = True
                finally:
                    app.state.pool.putconn(conn)
            except Exception:
                postgres_ok = False

        return HealthOut(
            status="ok" if postgres_ok else "degraded",
            postgres=postgres_ok,
            gold_hotspots_available=gold_hotspots_available(config.gold_hotspots_path),
        )

    return app


app = create_app()
