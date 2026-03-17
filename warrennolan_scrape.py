"""
Warren Nolan — 2026 Women's College Basketball Stats Scraper
=============================================================
Scrapes every stats page from WarrenNolan.com for the 2026 women's season,
filters to the 68-team NCAA tournament field, and writes a single merged
CSV: warrennolan_wbb2026_tournament.csv

Also writes warrennolan_wbb2026_all_teams.csv with the full D-I dataset.

REQUIREMENTS
------------
    pip install requests pandas beautifulsoup4 lxml

USAGE
-----
    python warrennolan_wbb2026_scraper.py
"""

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
    ("stats-adv-block-percent",            "blk_pct"),
    # ── Offense ───────────────────────────────────────────────────────────────
    ("stats-off-scoring-margin",           "scoring_margin"),
    ("stats-off-points-per-game",          "ppg"),
    ("stats-off-fg-percent",               "fg_pct"),
    ("stats-off-2p-percent",               "two_p_pct"),
    ("stats-off-3p-percent",               "three_p_pct"),
    ("stats-off-ft-percent",               "ft_pct"),
    ("stats-off-assists-per-game",         "apg"),
    ("stats-off-to-per-game",              "topg"),
    ("stats-off-assist-to-tov",            "ast_to_ratio"),
    # ── Defense ───────────────────────────────────────────────────────────────
    ("stats-def-points-per-game",          "opp_ppg"),
    ("stats-def-fg-percent",               "opp_fg_pct"),
    ("stats-def-2p-percent",               "opp_2p_pct"),
    ("stats-def-3p-percent",               "opp_3p_pct"),
    ("stats-def-steals",                   "spg"),
    ("stats-def-blocks",                   "bpg"),
    ("stats-def-fouls",                    "fpg"),
    # ── Season records ────────────────────────────────────────────────────────
    ("stats-season-win-percent",           "win_pct"),
    ("stats-season-most-wins",             "wins"),
    ("stats-season-most-q1-wins",          "q1_wins"),
    ("stats-season-best-q1-win-percent",   "q1_win_pct"),
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
    # ── 1 seeds ───────────────────────────────────────────────────────────────
    "UCLA": 1, "Texas": 1, "Connecticut": 1, "South Carolina": 1,
    # ── 2 seeds ───────────────────────────────────────────────────────────────
    "LSU": 2, "Michigan": 2, "Vanderbilt": 2, "Iowa": 2,
    # ── 3 seeds ───────────────────────────────────────────────────────────────
    "Duke": 3, "Louisville": 3, "Ohio State": 3, "TCU": 3,
    # ── 4 seeds ───────────────────────────────────────────────────────────────
    "Minnesota": 4, "West Virginia": 4, "North Carolina": 4, "Oklahoma": 4,
    # ── 5 seeds ───────────────────────────────────────────────────────────────
    "Ole Miss": 5, "Kentucky": 5, "Maryland": 5, "Michigan State": 5,
    # ── 6 seeds ───────────────────────────────────────────────────────────────
    "Baylor": 6, "Alabama": 6, "Notre Dame": 6, "Washington": 6,
    # ── 7 seeds ───────────────────────────────────────────────────────────────
    "Texas Tech": 7, "North Carolina State": 7, "Illinois": 7, "Georgia": 7,
    # ── 8 seeds ───────────────────────────────────────────────────────────────
    "Oklahoma State": 8, "Oregon": 8, "Iowa State": 8, "Clemson": 8,
    # ── 9 seeds ───────────────────────────────────────────────────────────────
    "Princeton": 9, "Virginia Tech": 9, "Syracuse": 9, "Southern California": 9,
    # ── 10 seeds — Virginia/Arizona State play First Four for one spot ────────
    "Villanova": 10, "Tennessee": 10, "Colorado": 10, "Virginia": 10, "Arizona State": 10,
    # ── 11 seeds — Nebraska/Richmond play First Four for one spot ─────────────
    "Nebraska": 11, "Richmond": 11, "Rhode Island": 11, "Fairfield": 11, "South Dakota State": 11,
    # ── 12 seeds ──────────────────────────────────────────────────────────────
    "Gonzaga": 12, "James Madison": 12, "Murray State": 12, "Colorado State": 12,
    # ── 13 seeds ──────────────────────────────────────────────────────────────
    "Green Bay": 13, "Miami (OH)": 13, "Western Illinois": 13, "Idaho": 13,
    # ── 14 seeds ──────────────────────────────────────────────────────────────
    "Charleston": 14, "Vermont": 14, "Howard": 14, "UC San Diego": 14,
    # ── 15 seeds ──────────────────────────────────────────────────────────────
    "Jacksonville": 15, "Holy Cross": 15, "High Point": 15, "Fairleigh Dickinson": 15,
    # ── 16 seeds — Missouri State/SFA and Southern/Samford play First Four ────
    "California Baptist": 16, "Missouri State": 16, "Stephen F. Austin": 16,
    "UTSA": 16, "Southern": 16, "Samford": 16,
}

# ─────────────────────────────────────────────────────────────────────────────
# HOME ARENA COORDINATES  (latitude, longitude)
# ─────────────────────────────────────────────────────────────────────────────

STADIUM_COORDS = {
    # 1 seeds
    "Connecticut":          (41.8084,  -72.2518),   # Gampel Pavilion, Storrs CT
    "UCLA":                 (34.0708, -118.4452),   # Pauley Pavilion, Los Angeles CA
    "Texas":                (30.2870,  -97.7365),   # Moody Center, Austin TX
    "South Carolina":       (33.9988,  -81.0368),   # Colonial Life Arena, Columbia SC
    # 2 seeds
    "LSU":                  (30.4122,  -91.1797),   # Pete Maravich Assembly Center, Baton Rouge LA
    "Michigan":             (42.2786,  -83.7487),   # Crisler Center, Ann Arbor MI
    "Vanderbilt":           (36.1439,  -86.8027),   # Memorial Gymnasium, Nashville TN
    "Iowa":                 (41.6596,  -91.5491),   # Carver-Hawkeye Arena, Iowa City IA
    # 3 seeds
    "Duke":                 (36.0001,  -78.9407),   # Cameron Indoor Stadium, Durham NC
    "Louisville":           (38.2543,  -85.7566),   # KFC Yum! Center, Louisville KY
    "Ohio State":           (40.0011,  -83.0198),   # Value City Arena, Columbus OH
    "TCU":                  (32.7098,  -97.3700),   # Schollmaier Arena, Fort Worth TX
    # 4 seeds
    "Minnesota":            (44.9751,  -93.2281),   # Williams Arena, Minneapolis MN
    "West Virginia":        (39.6480,  -79.9558),   # WVU Coliseum, Morgantown WV
    "North Carolina":       (35.9049,  -79.0469),   # Dean Smith Center, Chapel Hill NC
    "Oklahoma":             (35.1929,  -97.4521),   # Lloyd Noble Center, Norman OK
    # 5 seeds
    "Ole Miss":             (34.3615,  -89.5376),   # The Pavilion, Oxford MS
    "Kentucky":             (38.0278,  -84.4994),   # Memorial Coliseum, Lexington KY
    "Maryland":             (38.9897,  -76.9378),   # Xfinity Center, College Park MD
    "Michigan State":       (42.7270,  -84.4894),   # Breslin Center, East Lansing MI
    # 6 seeds
    "Baylor":               (31.5491,  -97.1151),   # Ferrell Center, Waco TX
    "Alabama":              (33.2093,  -87.5511),   # Coleman Coliseum, Tuscaloosa AL
    "Notre Dame":           (41.7010,  -86.2380),   # Purcell Pavilion, Notre Dame IN
    "Washington":           (47.6546, -122.3016),   # Alaska Airlines Arena, Seattle WA
    # 7 seeds
    "Texas Tech":           (33.5887, -101.8841),   # United Supermarkets Arena, Lubbock TX
    "North Carolina State": (35.7873,  -78.6870),   # Reynolds Coliseum, Raleigh NC
    "Illinois":             (40.0980,  -88.2722),   # State Farm Center, Champaign IL
    "Georgia":              (33.9452,  -83.3768),   # Stegeman Coliseum, Athens GA
    # 8 seeds
    "Oklahoma State":       (36.1258,  -97.0672),   # Gallagher-Iba Arena, Stillwater OK
    "Oregon":               (44.0521, -123.0684),   # Matthew Knight Arena, Eugene OR
    "Iowa State":           (42.0267,  -93.6465),   # Hilton Coliseum, Ames IA
    "Clemson":              (34.6776,  -82.8374),   # Littlejohn Coliseum, Clemson SC
    # 9 seeds
    "Princeton":            (40.3450,  -74.6550),   # Jadwin Gymnasium, Princeton NJ
    "Virginia Tech":        (37.2244,  -80.4246),   # Cassell Coliseum, Blacksburg VA
    "Syracuse":             (43.0361,  -76.1363),   # JMA Wireless Dome, Syracuse NY
    "Southern California":  (34.0239, -118.2836),   # Galen Center, Los Angeles CA
    # 10 seeds (Virginia/Arizona State play First Four for one spot)
    "Villanova":            (40.0352,  -75.3452),   # Finneran Pavilion, Villanova PA
    "Tennessee":            (35.9550,  -83.9257),   # Thompson-Boling Arena, Knoxville TN
    "Colorado":             (40.0076, -105.2659),   # CU Events Center, Boulder CO
    "Virginia":             (38.0356,  -78.5070),   # John Paul Jones Arena, Charlottesville VA
    "Arizona State":        (33.4256, -111.9327),   # Desert Financial Arena, Tempe AZ
    # 11 seeds (Nebraska/Richmond play First Four for one spot)
    "Nebraska":             (40.8035,  -96.7084),   # Pinnacle Bank Arena, Lincoln NE
    "Richmond":             (37.5701,  -77.5425),   # Robins Center, Richmond VA
    "Rhode Island":         (41.4807,  -71.5300),   # Ryan Center, Kingston RI
    "Fairfield":            (41.1548,  -73.2607),   # Alumni Hall, Fairfield CT
    "South Dakota State":   (44.3116,  -96.7984),   # Frost Arena, Brookings SD
    # 12 seeds
    "Gonzaga":              (47.6673, -117.4025),   # McCarthey Athletic Center, Spokane WA
    "James Madison":        (38.4356,  -78.8753),   # Atlantic Union Bank Center, Harrisonburg VA
    "Murray State":         (36.6177,  -88.3206),   # CFSB Center, Murray KY
    "Colorado State":       (40.5759, -105.0853),   # Moby Arena, Fort Collins CO
    # 13 seeds
    "Green Bay":            (44.5193,  -87.9956),   # Kress Events Center, Green Bay WI
    "Miami (OH)":           (39.5112,  -84.7346),   # Millett Hall, Oxford OH
    "Western Illinois":     (40.4614,  -90.6682),   # Western Hall, Macomb IL
    "Idaho":                (46.7254, -117.0097),   # ICCU Arena, Moscow ID
    # 14 seeds
    "Charleston":           (32.7765,  -79.9371),   # TD Arena, Charleston SC
    "Vermont":              (44.4759,  -73.1965),   # Patrick Gymnasium, Burlington VT
    "Howard":               (38.9222,  -77.0199),   # Burr Gymnasium, Washington DC
    "UC San Diego":         (32.8779, -117.2363),   # RIMAC Arena, La Jolla CA
    # 15 seeds
    "Jacksonville":         (30.3622,  -81.5910),   # Swisher Gymnasium, Jacksonville FL
    "Holy Cross":           (42.2509,  -71.8074),   # Hart Center, Worcester MA
    "High Point":           (35.9540,  -80.0029),   # Millis Athletic Center, High Point NC
    "Fairleigh Dickinson":  (40.8893,  -74.0279),   # Rothman Center, Teaneck NJ
    # 16 seeds (Missouri State/SFA and Southern/Samford play First Four for two spots)
    "California Baptist":   (33.9381, -117.4351),   # CBU Events Center, Riverside CA
    "Missouri State":       (37.2153,  -93.2981),   # JQH Arena, Springfield MO
    "Stephen F. Austin":    (31.6099,  -94.6563),   # William R. Johnson Coliseum, Nacogdoches TX
    "UTSA":                 (29.5800,  -98.6136),   # UTSA Convocation Center, San Antonio TX
    "Southern":             (30.5261,  -91.1890),   # F.G. Clark Activity Center, Baton Rouge LA
    "Samford":              (33.4623,  -86.7897),   # Pete Hanna Center, Birmingham AL
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
        import re
        s = s.strip().lower().replace("&", "and").replace(".", "")
        s = re.sub(r"[\-\(\)]", " ", s)   # hyphens and parens → space
        return re.sub(r"\s+", " ", s).strip()

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
    coords_df = pd.DataFrame(
        [(k, v[0], v[1]) for k, v in STADIUM_COORDS.items()],
        columns=["team", "stadium_lat", "stadium_lng"],
    )
    tourn_df = tourn_df.merge(coords_df, on="team", how="left")
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
