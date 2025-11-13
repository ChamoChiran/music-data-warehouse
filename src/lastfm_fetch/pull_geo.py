"""
pull_geo.py
Fetch country-level top artists or tracks from Last.fm,
convert response directly into a DataFrame,
and show the first 5 rows.

Usage:
    python -m src.lastfm_fetch.pull_geo --country "Japan" --type "artists"
"""

import requests
import typer
import pandas as pd
from datetime import datetime
from ..config import API_KEY, BASE_URL


app = typer.Typer()


# ---------------------------------------------------------
# Fetch API
# ---------------------------------------------------------
def fetch_geo_data(country: str, chart_type: str, limit: int, page: int):
    method = f"geo.gettop{chart_type}"

    params = {
        "method": method,
        "country": country,
        "api_key": API_KEY,
        "format": "json",
        "limit": limit,
        "page": page,
    }

    response = requests.get(BASE_URL, params=params, timeout=100)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------
# Extract DataFrame (Artists)
# ---------------------------------------------------------
def extract_artists_to_df(data: dict, country: str) -> pd.DataFrame:
    artists = data.get("topartists", {}).get("artist", [])

    if not artists:
        return pd.DataFrame()

    df = pd.json_normalize(artists)

    df.rename(
        columns={
            "@attr.rank": "rank",
            "name": "artist_name",
            "mbid": "artist_mbid",
            "listeners": "artist_listeners",
            "url": "artist_url",
        },
        inplace=True
    )

    df = df.drop(columns=["image"], errors="ignore")

    df["chart_country"] = country
    df["load_time"] = datetime.now()

    df["rank"] = df["rank"].astype(int)
    df["artist_listeners"] = df["artist_listeners"].astype(int)

    return df


# ---------------------------------------------------------
# Extract DataFrame (Tracks)
# ---------------------------------------------------------
def extract_tracks_to_df(data: dict, country: str) -> pd.DataFrame:
    tracks = data.get("toptracks", {}).get("track", [])

    if not tracks:
        return pd.DataFrame()

    df = pd.json_normalize(tracks)

    df.rename(
        columns={
            "@attr.rank": "rank",
            "name": "track_name",
            "artist.name": "artist_name",
            "artist.mbid": "artist_mbid",
            "listeners": "track_listeners",
            "playcount": "track_playcount",
            "url": "track_url",
        },
        inplace=True
    )

    df = df.drop(columns=["image"], errors="ignore")

    df["chart_country"] = country
    df["load_time"] = datetime.now()

    df["rank"] = df["rank"].astype(int)
    df["track_listeners"] = df["track_listeners"].astype(int)
    df["track_playcount"] = df["track_playcount"].astype(int)

    return df


# ---------------------------------------------------------
# CLI
# ---------------------------------------------------------
def main(
    country: str = typer.Option(..., "--country", "-c"),
    chart_type: str = typer.Option(..., "--type", "-t", help="'artists' or 'tracks'"),
    limit: int = typer.Option(20, "--limit", "-l"),
    page: int = typer.Option(1, "--page", "-p")
):
    if not API_KEY:
        typer.secho("LASTFM_API_KEY is missing!", fg=typer.colors.RED)
        raise typer.Exit()

    typer.echo(f"Fetching {chart_type} for {country}...")
    data = fetch_geo_data(country, chart_type, limit, page)

    if chart_type == "artists":
        df = extract_artists_to_df(data, country)
    else:
        df = extract_tracks_to_df(data, country)

    typer.echo(f"Extracted {len(df)} rows.")
    typer.echo("\n--- First 5 rows ---")
    print(df.head())


if __name__ == "__main__":
    typer.run(main)
