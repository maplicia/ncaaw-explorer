import time
import requests
import pandas as pd
from io import StringIO

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BASE = "https://www.warrennolan.com/basketballw/2026"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Seconds to wait between requests (be polite to the server)
REQUEST_DELAY = 1.0

# ─────────────────────────────────────────────────────────────────────────────
# STATS PAGES TO SCRAPE
# Each entry: (url_slug, column_name_in_csv)
# ─────────────────────────────────────────────────────────────────────────────

STATS_PAGES = [
    # ── Advanced ──────────────────────────────────────────────────────────────
    ("stats-adv-offensive-rating",         "off_rating"),
    ("stats-adv-defensive-rating",         "def_rating"),
    ("stats-adv-net-rating",               "net_rating"),
    ("stats-adv-pace",                     "pace"),
    ("stats-adv-efg-percent",              "efg_pct"),
    ("stats-adv-true-shooting-percent",    "ts_pct"),
    ("stats-adv-fta-rate",                 "fta_rate"),
    ("stats-adv-3pa-rate",                 "three_pa_rate"),
    ("stats-adv-orb-percent",              "orb_pct"),
    ("stats-adv-drb-percent",              "drb_pct"),
    ("stats-adv-total-rebound-percent",    "total_reb_pct"),
    ("stats-adv-assist-percent",           "ast_pct"),
    ("stats-adv-steal-percent",            "stl_pct"),
    ("stats-adv-block-percent",            "blk_pct")
]

# Also grab NET and RPI rankings from their own pages
RANKING_PAGES = [
    ("net",      "net_rank"),
    ("rpi-live", "rpi_rank"),
    ("elo",      "elo_rating"),
]

# ─────────────────────────────────────────────────────────────────────────────
# 2026 NCAA TOURNAMENT FIELD  (68 teams, seeds 1–16 × 4 regions)
# Key = Warren Nolan team name  |  Value = seed
# ─────────────────────────────────────────────────────────────────────────────

TOURNAMENT_TEAMS = {
    # 1 seeds
    "Connecticut": 1, "UCLA": 1, "Texas": 1, "South Carolina": 1,
    # 2 seeds
    "Iowa": 2, "Duke": 2, "LSU": 2, "Vanderbilt": 2,
    # 3 seeds
    "Oklahoma": 3, "North Carolina": 3, "Ohio State": 3, "Indiana": 3,
    # 4 seeds
    "West Virginia": 4, "Tennessee": 4, "Minnesota": 4, "Nebraska": 4,
    # 5 seeds
    "Ole Miss": 5, "Oregon": 5, "NC State": 5, "Iowa State": 5,
    # 6 seeds
    "Baylor": 6, "Georgia": 6, "Michigan": 6, "Arizona": 6,
    # 7 seeds
    "Illinois": 7, "Missouri": 7, "Virginia Tech": 7, "Michigan State": 7,
    # 8 seeds
    "Oklahoma State": 8, "Maryland": 8, "Kansas": 8, "Texas A&M": 8,
    # 9 seeds
    "Princeton": 9, "Florida State": 9, "Alabama": 9, "Wisconsin": 9,
    # 10 seeds
    "Gonzaga": 10, "South Dakota State": 10, "Colorado": 10, "Kansas State": 10,
    # 11 seeds (incl. First Four at-large play-ins)
    "Richmond": 11, "UTSA": 11, "Drake": 11, "Columbia": 11,
    # 12 seeds
    "Fairfield": 12, "Murray State": 12, "Rhode Island": 12, "James Madison": 12,
    # 13 seeds
    "Green Bay": 13, "Vermont": 13, "Missouri State": 13, "Samford": 13,
    # 14 seeds
    "Charleston": 14, "Colorado State": 14, "Holy Cross": 14, "Miami-OH": 14,
    # 15 seeds
    "UC San Diego": 15, "Idaho": 15, "Fairleigh Dickinson": 15, "Stephen F. Austin": 15,
    # 16 seeds (incl. First Four low-seed play-ins)
    "California Baptist": 16, "Howard": 16, "Southern": 16, "Western Illinois": 16,
}

# ─────────────────────────────────────────────────────────────────────────────
# SCRAPING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_page(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.text


def extract_ranked_table(html: str, stat_col: str) -> pd.DataFrame:
    """
    Parse a Warren Nolan ranked-stats page.

    Every stats page has a table with exactly 3 meaningful columns:
        rank  |  team  |  value

    Returns a DataFrame: [team, <stat_col>]
    """
    tables = pd.read_html(StringIO(html))
    if not tables:
        raise ValueError("No tables found on page.")

    # Take the largest table (the data table)
    df = max(tables, key=len).copy()

    # Flatten any MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]

    df.dropna(axis=1, how="all", inplace=True)

    # Find the team column: first column where most values are non-numeric strings
    team_col = None
    for col in df.columns:
        series = df[col].dropna().astype(str)
        is_non_numeric = series.apply(
            lambda x: not x.replace(".", "").replace("-", "").lstrip("-").isdigit()
        )
        if is_non_numeric.mean() > 0.6 and len(series) > 5:
            team_col = col
            break

    if team_col is None:
        raise ValueError(f"Cannot find team column. Columns: {list(df.columns)}")

    # Find the stat value column: last column that's mostly numeric (and not the rank)
    value_col = None
    for col in reversed(list(df.columns)):
        if col == team_col:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().mean() > 0.6:
            value_col = col
            break

    if value_col is None:
        raise ValueError(f"Cannot find value column. Columns: {list(df.columns)}")

    result = df[[team_col, value_col]].copy()
    result.columns = ["team", stat_col]
    result["team"] = result["team"].astype(str).str.strip()
    result[stat_col] = pd.to_numeric(result[stat_col], errors="coerce")
    result = result[result["team"].str.len() > 1].dropna(subset=[stat_col])
    result = result.drop_duplicates("team")

    return result


def extract_ranking_table(html: str, rank_col: str) -> pd.DataFrame:
    """
    Parse a ranking page (NET, RPI, ELO).
    These pages have more columns, but we just want team + the primary rank/rating value.
    """
    tables = pd.read_html(StringIO(html))
    if not tables:
        raise ValueError("No tables found on ranking page.")

    df = max(tables, key=len).copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]

    df.dropna(axis=1, how="all", inplace=True)

    # Find team column
    team_col = None
    for col in df.columns:
        series = df[col].dropna().astype(str)
        is_non_numeric = series.apply(
            lambda x: not x.replace(".", "").replace("-", "").lstrip("-").isdigit()
        )
        if is_non_numeric.mean() > 0.6 and len(series) > 5:
            team_col = col
            break

    if team_col is None:
        return pd.DataFrame(columns=["team", rank_col])

    # For ranking pages: first numeric col after team = the rank or primary rating
    value_col = None
    passed_team = False
    for col in df.columns:
        if col == team_col:
            passed_team = True
            continue
        if not passed_team:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().mean() > 0.6:
            value_col = col
            break

    if value_col is None:
        return pd.DataFrame(columns=["team", rank_col])

    result = df[[team_col, value_col]].copy()
    result.columns = ["team", rank_col]
    result["team"] = result["team"].astype(str).str.strip()
    result[rank_col] = pd.to_numeric(result[rank_col], errors="coerce")
    result = result[result["team"].str.len() > 1].dropna(subset=[rank_col])
    result = result.drop_duplicates("team")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def scrape_all() -> pd.DataFrame:
    """Scrape every stats page and merge into one wide DataFrame."""
    merged = None
    total = len(STATS_PAGES) + len(RANKING_PAGES)

    # ── Stats pages ───────────────────────────────────────────────────────────
    for i, (slug, col_name) in enumerate(STATS_PAGES, 1):
        url = f"{BASE}/{slug}"
        print(f"  [{i:2d}/{total}]  {col_name:<25}  {url}")
        try:
            html = fetch_page(url)
            df = extract_ranked_table(html, col_name)
            print(f"           → {len(df)} teams")
            merged = df if merged is None else pd.merge(merged, df, on="team", how="outer")
        except Exception as e:
            print(f"           ✗ FAILED: {e}")
        time.sleep(REQUEST_DELAY)

    # ── Ranking pages ─────────────────────────────────────────────────────────
    for i, (slug, col_name) in enumerate(RANKING_PAGES, len(STATS_PAGES) + 1):
        url = f"{BASE}/{slug}"
        print(f"  [{i:2d}/{total}]  {col_name:<25}  {url}")
        try:
            html = fetch_page(url)
            df = extract_ranking_table(html, col_name)
            print(f"           → {len(df)} teams")
            merged = pd.merge(merged, df, on="team", how="outer")
        except Exception as e:
            print(f"           ✗ FAILED: {e}")
        time.sleep(REQUEST_DELAY)

    return merged.sort_values("team").reset_index(drop=True)


def attach_seeds(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'seed' column; NaN for non-tournament teams."""
    def normalise(s):
        return s.strip().lower().replace("&", "and").replace(".", "").replace("-", " ")

    norm_map = {normalise(k): v for k, v in TOURNAMENT_TEAMS.items()}
    df = df.copy()
    df["seed"] = df["team"].apply(lambda t: norm_map.get(normalise(t), None))
    df["in_tournament"] = df["seed"].notna()
    # Move team/seed/in_tournament to front
    front = ["team", "seed", "in_tournament"]
    rest  = [c for c in df.columns if c not in front]
    return df[front + rest]


def main():
    print(f"\n{'='*65}")
    print("  Warren Nolan 2026 WBB Stats Scraper")
    print(f"{'='*65}\n")

    # ── Scrape ────────────────────────────────────────────────────────────────
    print(f"Scraping {len(STATS_PAGES) + len(RANKING_PAGES)} pages from WarrenNolan.com …\n")
    all_df = scrape_all()

    # ── Attach seeds & tournament flag ────────────────────────────────────────
    all_df = attach_seeds(all_df)

    total_teams = len(all_df)
    tourn_teams = all_df["in_tournament"].sum()
    print(f"\nTotal D-I teams scraped : {total_teams}")
    print(f"Tournament teams matched: {tourn_teams} / {len(TOURNAMENT_TEAMS)}")

    # ── Save full D-I CSV ─────────────────────────────────────────────────────
    all_out = "warrennolan_wbb2026_all_teams.csv"
    all_df.to_csv(all_out, index=False)
    print(f"\nSaved full D-I dataset  → {all_out}  ({total_teams} teams × {len(all_df.columns)} cols)")

    # ── Save tournament-only CSV ──────────────────────────────────────────────
    tourn_df = (
        all_df[all_df["in_tournament"]]
        .sort_values("seed")
        .reset_index(drop=True)
    )
    tourn_out = "warrennolan_wbb2026_tournament.csv"
    tourn_df.to_csv(tourn_out, index=False)
    print(f"Saved tournament dataset → {tourn_out}  ({len(tourn_df)} teams × {len(tourn_df.columns)} cols)")

    # ── Quick preview ─────────────────────────────────────────────────────────
    print(f"\n{'─'*65}")
    print("Tournament teams preview (seeds 1–4):")
    print(f"{'─'*65}")
    preview_cols = ["team", "seed", "net_rating", "off_rating", "def_rating",
                    "pace", "efg_pct", "net_rank"]
    available = [c for c in preview_cols if c in tourn_df.columns]
    print(tourn_df[tourn_df["seed"] <= 4][available].to_string(index=False))

    # Warn about any bracket teams not found in WN data
    missing = set(TOURNAMENT_TEAMS.keys()) - set(tourn_df["team"].values)
    if missing:
        print(f"\n⚠  {len(missing)} tournament teams not matched in WN data:")
        for t in sorted(missing):
            slug = t.replace(" ", "-").replace("&", "and")
            print(f"   '{t}'  →  verify at {BASE}/schedule/{slug}")
        print("   Tip: Update TOURNAMENT_TEAMS dict with the exact WN spelling.")


if __name__ == "__main__":
    main()
