import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class FPLDataScraper:
    """
    Scrapes Fantasy Premier League (FPL) data using only the official FPL API.

    What this class does:
    - Downloads the current "bootstrap-static" dataset (teams, players, season cumulative stats).
    - Detects the last completed gameweek.
    - Writes:
        - teams.csv
        - players.csv
        - playerstats_cumulative_GW{gw}.csv  (cumulative season totals at last completed GW)
        - player_gameweek_stats_GW{gw}.csv  (discrete GW stats, computed as diff vs previous GW snapshot)
        - player_gameweek_stats_last_gw.csv (same as above, but always the latest for convenience)

    How we get expected goals (xG, xA, xGI, xGC):
    - These are available in FPL "bootstrap-static" as cumulative season values.
    - Discrete gameweek values are computed by:
        GW_n = cumulative(GW_n) - cumulative(GW_{n-1})

    Extra fallback (first run / missing previous snapshot):
    - If the previous GW cumulative snapshot file is missing, we can optionally compute the last GW discrete
      stats from /element-summary/{player_id}/ for each player.
      This is slower (one request per player) but avoids needing historical snapshots.
    """

    FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
    FPL_ELEMENT_SUMMARY_URL = "https://fantasy.premierleague.com/api/element-summary/{player_id}/"

    # Columns used for identity (stable keys and names)
    ID_COLS = ["id", "first_name", "second_name", "web_name"]

    # "Snapshot" columns: values that are not intended to be differenced week-to-week
    # (ranks, percentages, per90 rates, news fields, etc.)
    SNAPSHOT_COLS = [
        "status",
        "news",
        "news_added",
        "now_cost",
        "now_cost_rank",
        "now_cost_rank_type",
        "selected_by_percent",
        "selected_rank",
        "selected_rank_type",
        "form",
        "form_rank",
        "form_rank_type",
        "event_points",
        "cost_change_event",
        "cost_change_event_fall",
        "cost_change_start",
        "cost_change_start_fall",
        "transfers_in_event",
        "transfers_out_event",
        "value_form",
        "value_season",
        "ep_next",
        "ep_this",
        "points_per_game",
        "points_per_game_rank",
        "points_per_game_rank_type",
        "chance_of_playing_next_round",
        "chance_of_playing_this_round",
        "influence_rank",
        "influence_rank_type",
        "creativity_rank",
        "creativity_rank_type",
        "threat_rank",
        "threat_rank_type",
        "ict_index_rank",
        "ict_index_rank_type",
        "corners_and_indirect_freekicks_order",
        "direct_freekicks_order",
        "penalties_order",
        "set_piece_threat",
        "corners_and_indirect_freekicks_text",
        "direct_freekicks_text",
        "penalties_text",
        "expected_goals_per_90",
        "expected_assists_per_90",
        "expected_goal_involvements_per_90",
        "expected_goals_conceded_per_90",
        "saves_per_90",
        "clean_sheets_per_90",
        "goals_conceded_per_90",
        "starts_per_90",
        "defensive_contribution_per_90",
    ]

    # "Cumulative" columns: values that increase over the season and should be differenced to get GW deltas
    # (includes expected_* totals)
    CUMULATIVE_COLS = [
        "total_points",
        "minutes",
        "goals_scored",
        "assists",
        "clean_sheets",
        "goals_conceded",
        "own_goals",
        "penalties_saved",
        "penalties_missed",
        "yellow_cards",
        "red_cards",
        "saves",
        "starts",
        "bonus",
        "bps",
        "transfers_in",
        "transfers_out",
        "dreamteam_count",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "influence",
        "creativity",
        "threat",
        "ict_index",
        "tackles",
        "clearances_blocks_interceptions",
        "recoveries",
        "defensive_contribution",
    ]

    def __init__(
            self,
            output_dir: str = "../data",
            request_timeout: int = 30,
            sleep_between_requests: float = 0.15,
            backfill_last_gw_if_missing_prev_snapshot: bool = True,
    ):
        """
        Initializes the scraper.

        Params:
        - output_dir: where CSV files will be written.
        - request_timeout: timeout (seconds) for HTTP requests.
        - sleep_between_requests: small delay between many requests (useful for element-summary fallback).
        - backfill_last_gw_if_missing_prev_snapshot: if True, and prior snapshot is missing,
          compute last GW discrete stats from element-summary endpoint (slower but works on first run).
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.request_timeout = request_timeout
        self.sleep = sleep_between_requests
        self.backfill = backfill_last_gw_if_missing_prev_snapshot

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,text/plain,*/*",
                "Origin": "https://fantasy.premierleague.com",
                "Referer": "https://fantasy.premierleague.com/",
            }
        )

    # -----------------------------
    # Low-level helpers
    # -----------------------------
    @staticmethod
    def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Ensures all requested columns exist in df (adds missing as NA), and returns df with those columns."""
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        return df[cols]

    @staticmethod
    def _to_numeric_series(s: pd.Series, default: float = 0.0) -> pd.Series:
        """
        Converts a pandas Series to numeric, coercing invalid values to NaN,
        then filling NaN with default.
        """
        return pd.to_numeric(s, errors="coerce").fillna(default)

    # -----------------------------
    # FPL fetching / parsing
    # -----------------------------
    def fetch_bootstrap(self) -> Dict:
        """Downloads the main FPL bootstrap-static payload (teams, players, events, etc.)."""
        logger.info("Fetching FPL API data...")
        r = self.session.get(self.FPL_BOOTSTRAP_URL, timeout=self.request_timeout)
        r.raise_for_status()
        logger.info("FPL API data fetched successfully")
        return r.json()

    def get_last_completed_gameweek(self, bootstrap: Dict) -> int:
        """
        Determines the last completed gameweek based on the 'events' array.
        Uses the last event that is finished and data_checked.
        """
        last_gw = None
        for ev in bootstrap.get("events", []):
            if ev.get("finished") and ev.get("data_checked"):
                last_gw = ev.get("id")
        return int(last_gw or 1)

    def extract_teams_df(self, bootstrap: Dict) -> pd.DataFrame:
        """Builds teams.csv dataset from bootstrap 'teams'."""
        teams = bootstrap.get("teams", [])
        rows = [
            {
                "team_id": t.get("id"),
                "name": t.get("name"),
                "short_name": t.get("short_name"),
            }
            for t in teams
        ]
        return pd.DataFrame(rows)

    def extract_players_df(self, bootstrap: Dict) -> pd.DataFrame:
        """
        Builds players.csv dataset from bootstrap 'elements'.
        Keeps a practical subset of fields that are useful for modeling and merges.
        """
        teams = {t["id"]: t for t in bootstrap.get("teams", [])}
        pos_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}

        rows = []
        for p in bootstrap.get("elements", []):
            team = teams.get(p.get("team"))
            rows.append(
                {
                    "id": p.get("id"),
                    "first_name": p.get("first_name"),
                    "second_name": p.get("second_name"),
                    "web_name": p.get("web_name"),
                    "team_id": p.get("team"),
                    "team_name": team.get("name") if team else None,
                    "team_short": team.get("short_name") if team else None,
                    "position": pos_map.get(p.get("element_type"), "UNK"),
                    "now_cost": p.get("now_cost"),
                    "minutes": p.get("minutes"),
                    "status": p.get("status"),
                }
            )
        return pd.DataFrame(rows)

    def build_cumulative_playerstats_snapshot(self, bootstrap: Dict, gw: int) -> pd.DataFrame:
        """
        Creates a per-player cumulative snapshot for the given gameweek 'gw'.

        This snapshot is the raw cumulative values from bootstrap elements at the time you run the script.
        It will include:
        - identity cols
        - gw column
        - snapshot cols (not differenced)
        - cumulative cols (differenced later)
        """
        elements_df = pd.DataFrame(bootstrap.get("elements", []))
        if elements_df.empty:
            return pd.DataFrame()

        # Ensure required columns exist and in consistent order
        wanted = self.ID_COLS + ["gw"] + self.SNAPSHOT_COLS + self.CUMULATIVE_COLS
        elements_df["gw"] = gw
        snap = self._ensure_columns(elements_df, wanted)

        # Convert cumulative numeric columns (FPL often returns them as strings)
        for col in self.CUMULATIVE_COLS:
            snap[col] = self._to_numeric_series(snap[col], default=0.0)

        # Some snapshot columns are numeric strings too; convert those that make sense
        numeric_like_snapshot = [
            "now_cost",
            "selected_by_percent",
            "form",
            "points_per_game",
            "ep_next",
            "ep_this",
        ]
        for col in numeric_like_snapshot:
            if col in snap.columns:
                snap[col] = pd.to_numeric(snap[col], errors="coerce")

        return snap

    # -----------------------------
    # Discrete GW stats calculation (same idea as your reference script)
    # -----------------------------
    def compute_discrete_gameweek_stats(self, current_snapshot: pd.DataFrame, prev_snapshot: pd.DataFrame) -> pd.DataFrame:
        """
        Computes discrete GW stats by differencing cumulative columns:
            discrete = current_cumulative - prev_cumulative

        Also includes defensive logic:
        - If a diff is negative (data correction), keep the current value as-is.
        """
        # Only merge what we need from the previous snapshot: id + cumulative fields
        prev_cols = ["id"] + [c for c in self.CUMULATIVE_COLS if c in prev_snapshot.columns]
        prev_small = prev_snapshot[prev_cols].copy()

        merged = current_snapshot.merge(prev_small, on="id", how="left", suffixes=("", "_prev"))

        for col in self.CUMULATIVE_COLS:
            prev_col = f"{col}_prev"
            if prev_col not in merged.columns:
                # If prev snapshot lacks the column, treat previous as 0
                merged[prev_col] = 0.0

            merged[prev_col] = self._to_numeric_series(merged[prev_col], default=0.0)
            merged[col] = self._to_numeric_series(merged[col], default=0.0)

            diff = merged[col] - merged[prev_col]

            # If diff is negative (rare data corrections), keep current as-is
            merged[col] = diff.where(diff >= 0, merged[col])

        # Output a clean, consistent schema:
        out_cols = self.ID_COLS + ["gw"] + self.SNAPSHOT_COLS + self.CUMULATIVE_COLS
        return self._ensure_columns(merged, out_cols)

    # -----------------------------
    # Optional fallback: build last-GW discrete stats directly from element-summary
    # -----------------------------
    def fetch_element_summary(self, player_id: int) -> Dict:
        """Fetches /element-summary/{player_id}/, which contains per-match history for that player."""
        url = self.FPL_ELEMENT_SUMMARY_URL.format(player_id=player_id)
        r = self.session.get(url, timeout=self.request_timeout)
        r.raise_for_status()
        return r.json()

    def backfill_last_gw_discrete_from_element_summary(
            self, bootstrap: Dict, last_gw: int
    ) -> pd.DataFrame:
        """
        Computes discrete stats for the last completed GW by calling element-summary for each player and summing
        entries where history.round == last_gw (handles double gameweeks).

        This is used only when we do not have a previous cumulative snapshot file to diff against.
        It is slower, but it provides correct last-GW xG/xA/xGI/xGC immediately.
        """
        logger.warning("Previous GW snapshot not found. Backfilling last GW discrete stats via element-summary (slow path).")

        # Build a base identity dataframe
        base_players = pd.DataFrame(bootstrap.get("elements", []))
        base_players = self._ensure_columns(base_players, self.ID_COLS + ["gw"])
        base_players["gw"] = last_gw

        # Prepare results container
        results: Dict[int, Dict] = {}

        # Only the cumulative-like fields we can reliably sum from match history for the GW
        # (these keys exist in element-summary history entries)
        hist_fields = [
            "minutes",
            "goals_scored",
            "assists",
            "clean_sheets",
            "goals_conceded",
            "own_goals",
            "penalties_saved",
            "penalties_missed",
            "yellow_cards",
            "red_cards",
            "saves",
            "bonus",
            "bps",
            "expected_goals",
            "expected_assists",
            "expected_goal_involvements",
            "expected_goals_conceded",
        ]

        ids = base_players["id"].dropna().astype(int).tolist()

        for i, pid in enumerate(ids, start=1):
            try:
                data = self.fetch_element_summary(pid)
                history = data.get("history", [])

                # sum all matches in the target round (DGW safe)
                gw_rows = [h for h in history if int(h.get("round", -1)) == int(last_gw)]

                agg = {k: 0.0 for k in hist_fields}
                for h in gw_rows:
                    for k in hist_fields:
                        agg[k] += float(pd.to_numeric(h.get(k, 0), errors="coerce") or 0.0)

                results[pid] = agg
            except Exception as e:
                logger.debug(f"element-summary failed for player {pid}: {e}")
                results[pid] = {k: 0.0 for k in hist_fields}

            # small pacing to reduce stress on the endpoint
            if self.sleep:
                time.sleep(self.sleep)

            if i % 100 == 0:
                logger.info(f"Backfill progress: {i}/{len(ids)} players")

        # Build discrete df from base identity + aggregated match stats
        discrete = base_players.copy()
        for k in hist_fields:
            discrete[k] = discrete["id"].map(lambda x: results.get(int(x), {}).get(k, 0.0) if pd.notna(x) else 0.0)

        # Add snapshot columns as NA (we don't derive them from element-summary)
        for c in self.SNAPSHOT_COLS:
            if c not in discrete.columns:
                discrete[c] = pd.NA

        # Ensure full schema and order
        out_cols = self.ID_COLS + ["gw"] + self.SNAPSHOT_COLS + self.CUMULATIVE_COLS
        # For cumulative columns not in hist_fields, they remain NA/0; keep them present for stable schema
        for c in self.CUMULATIVE_COLS:
            if c not in discrete.columns:
                discrete[c] = 0.0

        # Convert cumulative cols to numeric
        for c in self.CUMULATIVE_COLS:
            discrete[c] = self._to_numeric_series(discrete[c], default=0.0)

        return self._ensure_columns(discrete, out_cols)

    # -----------------------------
    # File IO and orchestration
    # -----------------------------
    def _path_cumulative_snapshot(self, gw: int) -> Path:
        """Returns the file path for the cumulative snapshot of a given GW."""
        return self.output_dir / f"playerstats_cumulative_GW{gw}.csv"

    def _path_discrete_gw_stats(self, gw: int) -> Path:
        """Returns the file path for the discrete (diffed) stats of a given GW."""
        return self.output_dir / f"player_gameweek_stats_GW{gw}.csv"

    def run(self) -> None:
        """
        Runs the full pipeline:
        1) fetch bootstrap
        2) determine last completed GW
        3) write teams.csv and players.csv
        4) write cumulative snapshot for last completed GW
        5) compute discrete GW stats:
           - fast path: diff vs previous cumulative snapshot
           - slow fallback (optional): element-summary backfill if previous snapshot missing
        6) write discrete GW CSV outputs
        """
        logger.info("=" * 60)
        logger.info("Starting FPL Data Scraper (FPL-only)")
        logger.info("=" * 60)

        bootstrap = self.fetch_bootstrap()
        last_gw = self.get_last_completed_gameweek(bootstrap)
        logger.info(f"Last completed gameweek: {last_gw}")

        # Save teams and players master files
        teams_df = self.extract_teams_df(bootstrap)
        players_df = self.extract_players_df(bootstrap)

        (self.output_dir / "teams.csv").write_text("", encoding="utf-8")  # ensures folder exists on some systems
        teams_df.to_csv(self.output_dir / "teams.csv", index=False)
        players_df.to_csv(self.output_dir / "players.csv", index=False)

        logger.info(f"Saved teams.csv and players.csv to {self.output_dir.resolve()}")

        # Build and save current cumulative snapshot
        current_snap = self.build_cumulative_playerstats_snapshot(bootstrap, gw=last_gw)
        if current_snap.empty:
            logger.error("No player data returned from bootstrap elements; aborting.")
            return

        current_snap_path = self._path_cumulative_snapshot(last_gw)
        current_snap.to_csv(current_snap_path, index=False)
        logger.info(f"Saved cumulative snapshot: {current_snap_path}")

        # Compute discrete GW stats
        prev_path = self._path_cumulative_snapshot(last_gw - 1)
        if prev_path.exists():
            prev_snap = pd.read_csv(prev_path)
            discrete = self.compute_discrete_gameweek_stats(current_snap, prev_snap)
            logger.info(f"Computed discrete GW stats via diff against: {prev_path.name}")
        else:
            if self.backfill:
                discrete = self.backfill_last_gw_discrete_from_element_summary(bootstrap, last_gw)
                logger.info("Computed discrete GW stats via element-summary backfill.")
            else:
                # Baseline fallback: treat current cumulative values as GW values (not correct if GW>1)
                logger.warning("Previous GW snapshot missing and backfill disabled; using cumulative as baseline GW values.")
                discrete = current_snap.copy()

        # Save discrete outputs
        discrete_path = self._path_discrete_gw_stats(last_gw)
        discrete.to_csv(discrete_path, index=False)
        discrete.to_csv(self.output_dir / "player_gameweek_stats_last_gw.csv", index=False)

        logger.info(f"Saved discrete GW stats: {discrete_path}")
        logger.info(f"Saved latest pointer file: {self.output_dir / 'player_gameweek_stats_last_gw.csv'}")

        logger.info("=" * 60)
        logger.info("Scraping completed successfully!")
        logger.info(f"Output directory: {self.output_dir.resolve()}")
        logger.info("=" * 60)

def main():
    # Make INFO logs visible in the terminal
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    scraper = FPLDataScraper(
        output_dir="../data",  # relative to scripts/ folder
        request_timeout=30,
        sleep_between_requests=0.15,
        backfill_last_gw_if_missing_prev_snapshot=True,
    )
    scraper.run()


if __name__ == "__main__":
    main()
