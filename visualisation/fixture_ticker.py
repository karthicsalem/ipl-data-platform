import os
from datetime import date

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="IPL 2026 Fixture Ticker", page_icon="🏏", layout="wide")

# ── Team Config ────────────────────────────────────────────────────────────────
TEAM_COLORS = {
    "Chennai Super Kings": "#F4C542",
    "Mumbai Indians": "#004BA0",
    "Royal Challengers Bengaluru": "#EC1C24",
    "Kolkata Knight Riders": "#3A225D",
    "Sunrisers Hyderabad": "#FF822A",
    "Rajasthan Royals": "#254AA5",
    "Delhi Capitals": "#0078BC",
    "Punjab Kings": "#ED1B24",
    "Lucknow Super Giants": "#A72056",
    "Gujarat Titans": "#1C1C1C",
}

TEAM_SHORT = {
    "Chennai Super Kings": "CSK",
    "Mumbai Indians": "MI",
    "Royal Challengers Bengaluru": "RCB",
    "Kolkata Knight Riders": "KKR",
    "Sunrisers Hyderabad": "SRH",
    "Rajasthan Royals": "RR",
    "Delhi Capitals": "DC",
    "Punjab Kings": "PBKS",
    "Lucknow Super Giants": "LSG",
    "Gujarat Titans": "GT",
}

TEAMS = list(TEAM_SHORT.keys())


# ── FDR Colors ─────────────────────────────────────────────────────────────────
def fdr_color(fdr):
    return {
        1: "#2d6a4f",  # dark green
        2: "#52b788",  # medium green
        3: "#e9c46a",  # muted yellow
        4: "#e76f51",  # muted orange
        5: "#c1121f",  # dark red
    }.get(fdr, "#6c757d")


def fdr_text_color(fdr):
    return "#ffffff" if fdr in [1, 4, 5] else "#000000"


# ── Load Schedule ──────────────────────────────────────────────────────────────
@st.cache_data
def load_schedule():
    path = "visualisation/temp_data/ipl_2026_schedule.csv"
    if not os.path.exists(path):
        st.error(f"Schedule file not found at {path}. Download from fixturedownload.com")
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df["date"] = pd.to_datetime(df["date"], dayfirst=True)
    return df.sort_values("date").reset_index(drop=True)


# ── Load Team Strength → FDR ───────────────────────────────────────────────────
@st.cache_data
def load_team_fdr():
    conn = duckdb.connect()
    wins = conn.execute("""
        SELECT match_winner AS team, COUNT(1) AS wins
        FROM read_csv('visualisation/temp_data/last_2_years.csv')
        WHERE season IN ('2024','2025')
        AND match_winner IS NOT NULL AND win_type != 'unknown'
        GROUP BY match_winner
    """).df()

    total = conn.execute("""
        SELECT team, COUNT(1) AS matches FROM (
            SELECT team_1 AS team FROM read_csv('visualisation/temp_data/last_2_years.csv')
            WHERE season IN ('2024','2025')
            UNION ALL
            SELECT team_2 AS team FROM read_csv('visualisation/temp_data/last_2_years.csv')
            WHERE season IN ('2024','2025')
        ) GROUP BY team
    """).df()

    strength = wins.merge(total, on="team")
    strength["win_rate"] = strength["wins"] / strength["matches"]
    mn, mx = strength["win_rate"].min(), strength["win_rate"].max()
    strength["fdr"] = ((strength["win_rate"] - mn) / (mx - mn) * 4 + 1).round(0).astype(int)
    return dict(zip(strength["team"], strength["fdr"]))


# ── Build Grid Data ────────────────────────────────────────────────────────────
def build_grid(schedule, fdr_map, start_date, end_date):
    """
    Returns dict: { team_name: { date: [(opponent, fdr, time)] } }
    """
    mask = (schedule["date"].dt.date >= start_date) & (schedule["date"].dt.date <= end_date)
    filtered = schedule[mask]

    grid = {team: {} for team in TEAMS}

    for _, row in filtered.iterrows():
        home = row.get("home_team", "")
        away = row.get("away_team", "")
        match_date = row["date"].date()
        time_str = str(row.get("time", "")).strip()

        # Find matching team names (partial match for robustness)
        home_full = next((t for t in TEAMS if t in home or home in t), home)
        away_full = next((t for t in TEAMS if t in away or away in t), away)

        home_fdr = fdr_map.get(away_full, 3)  # difficulty = opponent strength
        away_fdr = fdr_map.get(home_full, 3)

        if home_full in grid:
            grid[home_full].setdefault(match_date, []).append((away_full, home_fdr, time_str))
        if away_full in grid:
            grid[away_full].setdefault(match_date, []).append((home_full, away_fdr, time_str))

    return grid


# ── Render Badge ───────────────────────────────────────────────────────────────
def badge(opponent, fdr, time_str):
    short = TEAM_SHORT.get(opponent, opponent[:3].upper())
    color = fdr_color(fdr)
    text_color = fdr_text_color(fdr)
    tooltip = f"{short} (FDR {fdr}){' @ ' + time_str if time_str else ''}"
    return f"""
        <div title="{tooltip}" style="
            background:{color};
            color:{text_color};
            border-radius:5px;
            padding:3px 7px;
            font-size:12px;
            font-weight:700;
            text-align:center;
            display:inline-block;
            min-width:44px;
            margin:1px;
            cursor:default;
        ">{short}</div>
    """


# ── Render Table ───────────────────────────────────────────────────────────────
def render_ticker(grid, dates, highlight_teams, show_blanks):
    today = date.today()

    html = """
    <style>
        .ticker-wrap { overflow-x: auto; width: 100%; }
        .ticker { border-collapse: collapse; font-family: sans-serif; min-width: 100%; }
        .ticker th {
            background: var(--secondary-background-color);
            color: var(--text-color);
            font-size: 11px;
            padding: 6px 8px;
            text-align: center;
            white-space: nowrap;
            border-bottom: 2px solid var(--secondary-background-color);
            position: sticky;
            top: 0;
        }
        .ticker th.today-col {
            color: #2d6a4f;
            border-bottom: 2px solid #2d6a4f;
        }
        .ticker td {
            border-bottom: 1px solid var(--secondary-background-color);
            border-right: 1px solid var(--secondary-background-color);
            padding: 5px 6px;
            text-align: center;
            vertical-align: middle;
            min-width: 60px;
            background: var(--background-color);
        }
        .ticker td.today-col { background: rgba(82, 183, 136, 0.15); }
        .ticker td.blank {
            color: var(--text-color);
            opacity: 0.2;
            font-size: 11px;
        }
        .ticker td.team-cell {
            text-align: left;
            font-weight: 700;
            font-size: 13px;
            white-space: nowrap;
            padding: 6px 12px;
            position: sticky;
            left: 0;
            background: var(--secondary-background-color);
            color: var(--text-color);
            z-index: 1;
            min-width: 60px;
        }
        .ticker th.team-header {
            position: sticky;
            left: 0;
            background: var(--secondary-background-color);
            z-index: 2;
            text-align: left;
        }
        .ticker tr.highlighted td { background: var(--background-color); }
        .ticker tr.highlighted td.team-cell { background: var(--secondary-background-color); }
        .match-count {
            font-size: 10px;
            color: #888;
            margin-left: 4px;
        }
    </style>
    <div class="ticker-wrap">
    <table class="ticker">
    <thead><tr>
        <th class="team-header">Team</th>
    """

    for d in dates:
        is_today = d == today
        day_str = d.strftime("%a")
        date_str = d.strftime("%d %b")
        cls = "today-col" if is_today else ""
        html += f"<th class='{cls}'>{day_str}<br>{date_str}</th>"

    html += "</tr></thead><tbody>"

    for team in TEAMS:
        if highlight_teams and TEAM_SHORT.get(team) not in highlight_teams:
            if not show_blanks:
                continue

        team_data = grid.get(team, {})
        total_matches = sum(len(v) for v in team_data.values())
        is_highlighted = not highlight_teams or TEAM_SHORT.get(team) in highlight_teams
        row_class = "highlighted" if is_highlighted else ""
        team_color = TEAM_COLORS.get(team, "#ffffff")
        short = TEAM_SHORT.get(team, team[:3])
        opacity = "1" if is_highlighted else "0.35"

        html += f"<tr class='{row_class}' style='opacity:{opacity}'>"
        html += f"""
            <td class="team-cell">
                <span style="color:{team_color}">■</span> {short}
                <span class="match-count">({total_matches})</span>
            </td>
        """

        for d in dates:
            is_today = d == today
            col_cls = "today-col" if is_today else ""
            matches = team_data.get(d, [])

            if matches:
                badges = "".join(badge(opp, fdr, t) for opp, fdr, t in matches)
                # DGW flag — team has 2+ matches in selected window
                dgw_flag = "<div style='font-size:9px;color:#4fc3f7'>2x 🎯</div>" if total_matches >= 2 else ""
                html += f"<td class='{col_cls}'>{badges}{dgw_flag}</td>"
            else:
                html += f"<td class='{col_cls} blank'>—</td>"

        html += "</tr>"

    html += "</tbody></table></div>"
    return html


# ── FDR Legend ─────────────────────────────────────────────────────────────────
def render_legend():
    items = [(1, "Easy"), (2, "Moderate"), (3, "Average"), (4, "Tough"), (5, "Hardest")]
    cols = st.columns(5)
    for col, (fdr, label) in zip(cols, items):
        tc = "#000" if fdr <= 3 else "#fff"
        col.markdown(
            f"<div style='background:{fdr_color(fdr)};color:{tc};"
            f"padding:5px;border-radius:5px;text-align:center;"
            f"font-size:12px;font-weight:700'>{fdr} — {label}</div>",
            unsafe_allow_html=True,
        )


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    st.title("🏏 IPL 2026 — Fixture Ticker")
    st.caption("Opponent badge colour = fixture difficulty (FDR). 🎯 2x = team has 2+ matches this week.")

    schedule = load_schedule()
    if schedule.empty:
        return

    fdr_map = load_team_fdr()

    # ── Sidebar Controls ───────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Controls")

        season_start = schedule["date"].min().date()
        season_end = schedule["date"].max().date()

        default_start = max(date.today(), season_start)
        date_range = st.date_input(
            "Date range", value=(default_start, season_end), min_value=season_start, max_value=season_end
        )

        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = date.today()
            end_date = season_end

        highlight_teams = st.multiselect(
            "Highlight teams",
            options=list(TEAM_SHORT.values()),
            default=[],
            help="Highlight specific teams. Others fade out.",
        )

        show_blanks = st.checkbox("Show all teams when filtering", value=True)

        st.divider()
        st.subheader("FDR Legend")
        for fdr, label in [(1, "Easy"), (2, "Moderate"), (3, "Average"), (4, "Tough"), (5, "Hardest")]:
            tc = "#000" if fdr <= 3 else "#fff"
            st.markdown(
                f"<div style='background:{fdr_color(fdr)};color:{tc};"
                f"padding:4px 8px;border-radius:4px;margin:2px 0;"
                f"font-size:12px;font-weight:700'>{fdr} — {label}</div>",
                unsafe_allow_html=True,
            )

    # ── Stats Bar ──────────────────────────────────────────────────────────────
    total_days = (end_date - start_date).days + 1
    mask = (schedule["date"].dt.date >= start_date) & (schedule["date"].dt.date <= end_date)
    total_matches = mask.sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Days shown", total_days)
    c2.metric("Matches in range", total_matches)
    c3.metric("Match days", schedule[mask]["date"].dt.date.nunique())

    st.divider()

    # ── Build & Render Grid ────────────────────────────────────────────────────
    all_dates = pd.date_range(start_date, end_date).date
    match_days = set(schedule[mask]["date"].dt.date.tolist())

    # Only show dates that have matches (skip blank days)
    show_all_days = st.checkbox("Show all days (including no-match days)", value=False)
    dates_to_show = list(all_dates) if show_all_days else [d for d in all_dates if d in match_days]

    grid = build_grid(schedule, fdr_map, start_date, end_date)
    html = render_ticker(grid, dates_to_show, highlight_teams, show_blanks)
    st.markdown(html, unsafe_allow_html=True)

    st.divider()

    # ── Fixture Density Table ──────────────────────────────────────────────────
    st.subheader("📊 Fixture Density", anchor=False)
    st.caption("Number of matches per team in the selected date range")

    density = []
    for team in TEAMS:
        team_data = grid.get(team, {})
        count = sum(len(v) for v in team_data.values())
        density.append(
            {
                "Team": TEAM_SHORT.get(team, team),
                "Matches": count,
            }
        )

    density_df = pd.DataFrame(density).sort_values("Matches", ascending=False)
    st.dataframe(density_df, width="content", hide_index=True)


if __name__ == "__main__":
    main()
