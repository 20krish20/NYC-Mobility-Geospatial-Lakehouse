"""Streamlit dashboard for the NYC Mobility Lakehouse.

Pulls everything from the FastAPI serving layer (``serving.api``):
- a live map of stations colored by ``imbalance_ratio``
- an H3 hexagon heatmap of demand hotspots
- borough-level summary tables
"""

from __future__ import annotations

import os

import h3
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

from streaming.h3_config import H3_RESOLUTION

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

# /stations/nearby requires a center + radius (there is no unfiltered
# "/stations" endpoint). A 50km radius from the city centroid covers all
# five boroughs and is the max radius the API allows.
NYC_CENTER_LAT = 40.7128
NYC_CENTER_LON = -74.0060
NYC_RADIUS_M = 50_000

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

# Red = empty (needs bikes), teal = balanced, blue = full (needs docks) --
# shared between the station map legend and the hotspot status legend.
STATUS_COLORS = {
    "empty": [220, 50, 47],
    "normal": [42, 161, 152],
    "full": [38, 139, 210],
    "mixed": [150, 150, 150],
}


@st.cache_data(ttl=30)
def fetch_json(path: str, params: dict | None = None) -> object | None:
    """GET ``path`` from the API, returning ``None`` on any connection error."""
    try:
        response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def fetch_health() -> dict | None:
    return fetch_json("/health")


def fetch_stations() -> pd.DataFrame:
    data = fetch_json(
        "/stations/nearby",
        {"lat": NYC_CENTER_LAT, "lon": NYC_CENTER_LON, "radius_m": NYC_RADIUS_M, "limit": 500},
    )
    return pd.DataFrame(data or [])


def fetch_hotspots(borough: str | None) -> pd.DataFrame:
    params = {"borough": borough} if borough else None
    return pd.DataFrame(fetch_json("/hotspots", params) or [])


def fetch_h3_cells(resolution: int, borough: str | None) -> pd.DataFrame:
    params = {"borough": borough} if borough else None
    return pd.DataFrame(fetch_json(f"/h3/{resolution}/cells", params) or [])


def imbalance_color(ratio: float) -> list[int]:
    """Map an imbalance ratio in [0, 1] to a red -> teal -> blue color."""
    if pd.isna(ratio):
        return STATUS_COLORS["mixed"]

    ratio = max(0.0, min(1.0, ratio))
    empty, normal, full = STATUS_COLORS["empty"], STATUS_COLORS["normal"], STATUS_COLORS["full"]
    if ratio <= 0.5:
        low, high, t = empty, normal, ratio / 0.5
    else:
        low, high, t = normal, full, (ratio - 0.5) / 0.5
    return [int(low[i] + t * (high[i] - low[i])) for i in range(3)]


def h3_cell_polygon(cell: str) -> list[list[float]]:
    """H3 cell boundary as a [lon, lat] ring for pydeck's PolygonLayer."""
    return [[lon, lat] for lat, lon in h3.cell_to_boundary(cell)]


def render_station_map(stations_df: pd.DataFrame) -> None:
    if stations_df.empty:
        st.info("No stations available yet. Has the snapshot writer run?")
        return

    df = stations_df.copy()
    df["color"] = df["imbalance_ratio"].apply(imbalance_color)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius=80,
        pickable=True,
    )
    view_state = pdk.ViewState(latitude=NYC_CENTER_LAT, longitude=NYC_CENTER_LON, zoom=10)
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={
                "text": "{name}\nbikes: {num_bikes_available}  docks: {num_docks_available}"
                "\nimbalance: {imbalance_ratio}"
            },
        )
    )
    st.caption(
        "Color scale: red = empty (needs bikes) -> teal = balanced -> blue = full (needs docks)"
    )


def render_h3_heatmap(resolution: int, borough: str | None) -> None:
    cells_df = fetch_h3_cells(resolution, borough)
    if cells_df.empty:
        st.info("No hotspot data available yet. Has the streaming job run?")
        return

    df = cells_df.copy()
    df["polygon"] = df["h3_index"].apply(h3_cell_polygon)
    df["color"] = (
        df["status"]
        .map(STATUS_COLORS)
        .apply(lambda c: c if isinstance(c, list) else STATUS_COLORS["mixed"])
    )

    layer = pdk.Layer(
        "PolygonLayer",
        data=df,
        get_polygon="polygon",
        get_fill_color="color",
        get_line_color=[255, 255, 255],
        line_width_min_pixels=1,
        opacity=0.6,
        pickable=True,
    )
    view_state = pdk.ViewState(latitude=NYC_CENTER_LAT, longitude=NYC_CENTER_LON, zoom=10)
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={
                "text": "{h3_index}\nstatus: {status}\navg ratio: {avg_ratio}"
                "\nreadings: {num_readings}"
            },
        )
    )

    legend = " &nbsp;&nbsp; ".join(
        f"<span style='color: rgb({r},{g},{b})'>■</span> {status}"
        for status, (r, g, b) in STATUS_COLORS.items()
    )
    st.markdown(legend, unsafe_allow_html=True)


def render_borough_summary(stations_df: pd.DataFrame, hotspots_df: pd.DataFrame) -> None:
    st.subheader("Station status by borough")
    if stations_df.empty:
        st.info("No station data available yet.")
    else:
        summary = (
            stations_df.groupby("borough", dropna=False)
            .agg(
                stations=("station_id", "count"),
                avg_imbalance_ratio=("imbalance_ratio", "mean"),
                total_bikes_available=("num_bikes_available", "sum"),
                total_docks_available=("num_docks_available", "sum"),
            )
            .reset_index()
        )
        st.dataframe(summary, use_container_width=True)

    st.subheader("Active hotspot cells by borough")
    if hotspots_df.empty:
        st.info("No hotspot data available yet.")
    else:
        hotspot_summary = (
            hotspots_df.groupby(["borough", "status"]).size().reset_index(name="cells")
        )
        st.dataframe(hotspot_summary, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="NYC Mobility Lakehouse", layout="wide")
    st.title("NYC Mobility Lakehouse")
    st.caption("Live Citi Bike station status, demand hotspots, and borough summaries.")

    health = fetch_health()
    if health is None:
        st.error(f"Cannot reach the API at {API_BASE_URL}. Is it running?")
        return

    status_cols = st.columns(3)
    status_cols[0].metric("API status", health["status"])
    status_cols[1].metric("Postgres", "up" if health["postgres"] else "down")
    status_cols[2].metric(
        "Gold hotspots", "available" if health["gold_hotspots_available"] else "unavailable"
    )

    st.sidebar.header("Filters")
    borough_choice = st.sidebar.selectbox("Borough", ["All", *BOROUGHS])
    borough = None if borough_choice == "All" else borough_choice
    resolution = st.sidebar.slider(
        "H3 resolution", min_value=5, max_value=H3_RESOLUTION, value=H3_RESOLUTION
    )

    stations_df = fetch_stations()
    if borough is not None and not stations_df.empty:
        stations_df = stations_df[stations_df["borough"] == borough]

    hotspots_df = fetch_hotspots(borough)

    station_tab, hotspot_tab, summary_tab = st.tabs(
        ["Live Station Map", "H3 Hotspot Heatmap", "Borough Summary"]
    )
    with station_tab:
        render_station_map(stations_df)
    with hotspot_tab:
        render_h3_heatmap(resolution, borough)
    with summary_tab:
        render_borough_summary(stations_df, hotspots_df)


if __name__ == "__main__":
    main()
