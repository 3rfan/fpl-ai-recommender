"""
FPL Data Scraper - Fetches player data from FPL API and fbref.com
Outputs CSV files for teams, players, and player statistics.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FPLDataScraper:
    """Scraper for FPL and fbref.com data."""

    FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
    FBREF_PL_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"

    def __init__(self, output_dir: str = "../data"):
        """Initialize scraper with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def fetch_fpl_data(self):
        """Fetch data from official FPL API."""
        logger.info("Fetching FPL API data...")
        try:
            response = self.session.get(self.FPL_API_URL, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info("FPL API data fetched successfully")
            return data
        except requests.RequestException as e:
            logger.error(f"Failed to fetch FPL API: {e}")
            sys.exit(1)

    def get_last_completed_gameweek(self, fpl_data):
        """Determine last completed gameweek from FPL API."""
        events = fpl_data.get('events', [])
        for event in events:
            if event.get('finished') and event.get('data_checked'):
                last_gw = event.get('id')

        if last_gw:
            logger.info(f"Last completed gameweek: {last_gw}")
            return last_gw
        else:
            logger.warning("No completed gameweek found, defaulting to 1")
            return 1

    def extract_teams(self, fpl_data):
        """Extract team data from FPL API."""
        logger.info("Extracting teams data...")
        teams = fpl_data.get('teams', [])

        teams_list = []
        for team in teams:
            teams_list.append({
                'fpl_code': team.get('id'),
                'name': team.get('name'),
                'short_name': team.get('short_name'),
            })

        df = pd.DataFrame(teams_list)
        logger.info(f"Extracted {len(df)} teams")
        return df

    def extract_players(self, fpl_data):
        """Extract player data from FPL API."""
        logger.info("Extracting players data...")
        players = fpl_data.get('elements', [])
        teams = {t['id']: t for t in fpl_data.get('teams', [])}

        # Position mapping: 1=GK, 2=DEF, 3=MID, 4=FWD
        position_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}

        players_list = []
        skipped_count = 0

        for player in players:
            # Data validation
            if not player.get('web_name') or player.get('now_cost') is None:
                logger.warning(f"Skipping player with missing name or price: {player}")
                skipped_count += 1
                continue

            team_id = player.get('team')
            team_name = teams.get(team_id, {}).get('name', 'Unknown')
            team_short = teams.get(team_id, {}).get('short_name', 'UNK')

            players_list.append({
                'fpl_id': player.get('id'),
                'name': player.get('web_name'),
                'full_name': f"{player.get('first_name', '')} {player.get('second_name', '')}".strip(),
                'team_name': team_name,
                'team_short': team_short,
                'team_fpl_code': team_id,
                'position': position_map.get(player.get('element_type'), 'UNK'),
                'price': player.get('now_cost') / 10.0,  # Convert to pounds
                'total_points': player.get('total_points', 0),
                'minutes': player.get('minutes', 0),
            })

        df = pd.DataFrame(players_list)
        logger.info(f"Extracted {len(df)} players (skipped {skipped_count})")
        return df

    def scrape_fbref_stats(self, last_gameweek):
        """Scrape player statistics from fbref.com."""
        logger.info("Scraping fbref.com Premier League stats...")

        try:
            # Add delay to be respectful to the server
            time.sleep(2)
            response = self.session.get(self.FBREF_PL_URL, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')

            # Find the main stats table
            stats_table = soup.find('table', {'class': 'stats_table'})

            if not stats_table:
                logger.error("Could not find stats table on fbref.com")
                return pd.DataFrame()

            # Parse table rows
            stats_list = []
            rows = stats_table.find('tbody').find_all('tr')

            for row in rows:
                # Skip header rows
                if row.get('class') and 'thead' in row.get('class'):
                    continue

                cells = row.find_all(['th', 'td'])
                if len(cells) < 10:
                    continue

                try:
                    player_name = cells[0].get_text(strip=True)
                    team = cells[1].get_text(strip=True) if len(cells) > 1 else ''

                    # Extract numeric stats (simplified for now)
                    stats_list.append({
                        'player_name': player_name,
                        'team': team,
                        'gameweek': last_gameweek,
                        'minutes': self._safe_int(cells, 5, 0),
                        'goals': self._safe_int(cells, 6, 0),
                        'assists': self._safe_int(cells, 7, 0),
                        'shots': self._safe_int(cells, 8, 0),
                        'key_passes': self._safe_int(cells, 9, 0),
                        'xg': self._safe_float(cells, 10, 0.0),
                        'xa': self._safe_float(cells, 11, 0.0),
                        'clean_sheet': False  # Will be computed based on team defensive data
                    })
                except Exception as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue

            df = pd.DataFrame(stats_list)
            logger.info(f"Scraped {len(df)} player stat records from fbref.com")
            return df

        except requests.RequestException as e:
            logger.error(f"Failed to scrape fbref.com: {e}")
            return pd.DataFrame()

    def _safe_int(self, cells, index, default=0):
        """Safely extract integer value from table cell."""
        try:
            if index < len(cells):
                text = cells[index].get_text(strip=True)
                return int(text) if text else default
        except (ValueError, AttributeError):
            pass
        return default

    def _safe_float(self, cells, index, default=0.0):
        """Safely extract float value from table cell."""
        try:
            if index < len(cells):
                text = cells[index].get_text(strip=True)
                return float(text) if text else default
        except (ValueError, AttributeError):
            pass
        return default

    def merge_player_stats(self, players_df, stats_df):
        """Merge FPL player data with fbref stats."""
        logger.info("Merging FPL and fbref data...")

        if stats_df.empty:
            logger.warning("No fbref stats to merge, creating empty stats file")
            # Create placeholder stats using FPL data
            stats_df = players_df[['fpl_id', 'name']].copy()
            stats_df['gameweek'] = 1
            stats_df['minutes'] = 0
            stats_df['goals'] = 0
            stats_df['assists'] = 0
            stats_df['shots'] = 0
            stats_df['key_passes'] = 0
            stats_df['xg'] = 0.0
            stats_df['xa'] = 0.0
            stats_df['clean_sheet'] = False
            return stats_df

        # Simple name matching (can be improved with fuzzy matching)
        merged = stats_df.copy()
        merged['fpl_id'] = None

        for idx, stat_row in stats_df.iterrows():
            player_name = stat_row['player_name'].lower()
            # Try to match with FPL players
            match = players_df[
                players_df['name'].str.lower().str.contains(player_name[:5], na=False) |
                players_df['full_name'].str.lower().str.contains(player_name[:8], na=False)
            ]

            if not match.empty:
                merged.at[idx, 'fpl_id'] = match.iloc[0]['fpl_id']

        # Filter out unmatched players
        merged = merged[merged['fpl_id'].notna()]
        logger.info(f"Merged {len(merged)} player stat records")

        return merged

    def save_to_csv(self, teams_df, players_df, stats_df):
        """Save dataframes to CSV files."""
        logger.info("Saving data to CSV files...")

        teams_file = self.output_dir / "teams.csv"
        players_file = self.output_dir / "players.csv"
        stats_file = self.output_dir / "player_stats_last_gw.csv"

        teams_df.to_csv(teams_file, index=False)
        players_df.to_csv(players_file, index=False)
        stats_df.to_csv(stats_file, index=False)

        logger.info(f"Saved teams to {teams_file}")
        logger.info(f"Saved players to {players_file}")
        logger.info(f"Saved player stats to {stats_file}")

    def run(self):
        """Execute the full scraping pipeline."""
        logger.info("=" * 60)
        logger.info("Starting FPL Data Scraper")
        logger.info("=" * 60)

        # Fetch FPL API data
        fpl_data = self.fetch_fpl_data()
        last_gw = self.get_last_completed_gameweek(fpl_data)

        # Extract teams and players
        teams_df = self.extract_teams(fpl_data)
        players_df = self.extract_players(fpl_data)

        # Scrape fbref stats
        stats_df = self.scrape_fbref_stats(last_gw)

        # Merge data
        merged_stats = self.merge_player_stats(players_df, stats_df)

        # Save to CSV
        self.save_to_csv(teams_df, players_df, merged_stats)

        logger.info("=" * 60)
        logger.info("Scraping completed successfully!")
        logger.info(f"Output directory: {self.output_dir.absolute()}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    scraper = FPLDataScraper()
    scraper.run()


if __name__ == "__main__":
    main()

