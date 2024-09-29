import requests
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests import RequestException, HTTPError, Timeout


class ChessRankingsServiceError(Exception):
    """Base class for exceptions in ChessRankingsService."""
    pass


class APIError(ChessRankingsServiceError):
    """Raised when there is an error with the API request."""
    def __init__(self, message: str, url: str):
        self.message = message
        self.url = url
        super().__init__(f"APIError: {message} | URL: {url}")


class PlayerNotFoundError(ChessRankingsServiceError):
    """Raised when no players are found on the leaderboard."""
    def __init__(self, message: str = "No players found on the leaderboard"):
        super().__init__(message)


class RatingHistoryNotFoundError(ChessRankingsServiceError):
    """Raised when the rating history for a player cannot be found."""
    def __init__(self, username: str):
        super().__init__(f"Rating history not found for user '{username}'")


class ClassicalRatingNotFoundError(ChessRankingsServiceError):
    """Raised when no classical rating history is found for the player."""
    def __init__(self, username: str):
        super().__init__(f"No Classical rating history found for user '{username}'")


class ChessRankingsService:
    BASE_URL = 'https://lichess.org/api'

    def print_top_50_classical_players(self) -> None:
        """
        Print the usernames of the top 50 classical chess players from Lichess.
        """
        usernames = self._get_top_players_usernames(50)
        for username in usernames:
            print(username)

    def print_last_30_day_rating_for_top_player(self) -> None:
        """
        Fetch and print the rating history for the top classical chess player over the last 30 days.
        """
        username = self._get_top_players_usernames(1)[0]
        ratings = self._get_last_30_days_classical_ratings_for_player(username)
        date_labels = [(datetime.now().date() - timedelta(days=i)).strftime('%b %d') for i in range(30, -1, -1)]
        output_ratings = dict(zip(date_labels, ratings))
        print(f"{username}, {output_ratings}")

    def generate_rating_csv_for_top_50_classical_players(self, filename: str = 'top_50_classical_players_ratings.csv') -> None:
        """
        Create a CSV that shows the rating history for each of the top 50 classical players for the last 30 days.
        The CSV will have 51 rows (1 header + 50 players) and 32 columns (username + 30 last days of ratings + today's rating).
        """
        usernames = self._get_top_players_usernames(50)

        today = datetime.now().date()
        date_range = [today - timedelta(days=i) for i in range(30, -1, -1)]
        header = ['username'] + [date.strftime('%Y-%m-%d') for date in date_range]

        data_rows = []

        max_workers = min(10, len(usernames))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_username = {
                executor.submit(
                    self._get_last_30_days_classical_ratings_for_player, username
                ): username for username in usernames
            }
            for future in as_completed(future_to_username):
                username = future_to_username[future]
                try:
                    player_ratings = future.result()
                    row = [username] + player_ratings
                except ChessRankingsServiceError as e:
                    print(f"An error occurred while processing user {username}: {e}")
                    row = [username] + [None] * 31

                data_rows.append(row)

        username_order = {username: index for index, username in enumerate(usernames)}
        data_rows.sort(key=lambda row: username_order.get(row[0], 0))

        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                for row in data_rows:
                    writer.writerow(row)
            print(f"CSV file '{filename}' has been created successfully.")
        except IOError as e:
            print(f"An error occurred while writing to the file '{filename}': {e}")

    def _get_last_30_days_classical_ratings_for_player(self, username: str) -> Dict[str, int]:
        rating_data = self._get_player_rating_history(username)
        classical_history = self._extract_classical_rating_history(rating_data, username)
        rating_dict = self._build_date_to_rating_mapping(classical_history)
        ratings = self._generate_last_30_days_ratings(rating_dict)
        return ratings

    def _get_top_players_usernames(self, count: int = 50) -> List[str]:
        """
        Retrieve the usernames of the top classical chess players from Lichess.
        """
        leaderboard_url = f'{self.BASE_URL}/player/top/{count}/classical'
        data = self._fetch_json(leaderboard_url)
        
        if data is None or 'users' not in data:
            raise PlayerNotFoundError("Could not retrieve players from the leaderboard.")

        players = data.get('users', [])
        if not players:
            raise PlayerNotFoundError()

        usernames = [player.get('username') for player in players if player.get('username')]
        return usernames

    def _fetch_json(self, url: str) -> Any:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except (RequestException, HTTPError, Timeout) as e:
            raise APIError(f"An error occurred while fetching data: {str(e)}", url)

    def _get_player_rating_history(self, username: str) -> List[Dict]:
        """
        Fetch the rating history for the specified user.
        """
        rating_history_url = f'{self.BASE_URL}/user/{username}/rating-history'
        data = self._fetch_json(rating_history_url)
        
        if data is None:
            raise RatingHistoryNotFoundError(username)
        
        return data

    def _extract_classical_rating_history(self, rating_data: List[Dict], username: str) -> List[List[int]]:
        for variant in rating_data:
            if variant.get('name') == 'Classical':
                return variant.get('points', [])
        raise ClassicalRatingNotFoundError(username)

    def _build_date_to_rating_mapping(self, classical_history: List[List[int]]) -> Dict[datetime.date, int]:
        rating_dict = {}
        for point in classical_history:
            year, month, day, rating = point
            # Adjust month from zero-indexed to one-indexed (0 = January)
            date = datetime(year, month + 1, day).date()
            rating_dict[date] = rating
        return rating_dict

    def _generate_last_30_days_ratings(self, rating_dict: Dict[datetime.date, int]) -> List[Optional[int]]:
        """
        Generate the rating history for the last 30 calendar days.
        If the player didn't play on a given day, the rating stays the same as the last known rating.
        Returns a list of ratings corresponding to each day.
        """
        today = datetime.now().date()
        last_30_days = [today - timedelta(days=i) for i in range(30, -1, -1)]

        start_date = last_30_days[0]
        last_known_rating = rating_dict.get(start_date)
        if last_known_rating is None:
            last_known_rating = self._get_first_rating_before_date(rating_dict, start_date)

        ratings = []
        for date in last_30_days:
            if date in rating_dict:
                last_known_rating = rating_dict[date]
            ratings.append(last_known_rating)
        return ratings

    def _get_first_rating_before_date(self, rating_dict: Dict[datetime.date, int], target_date: datetime.date) -> Optional[int]:
        dates_before_target = [date for date in rating_dict.keys() if date < target_date]
        if dates_before_target:
            most_recent_date = max(dates_before_target)
            return rating_dict[most_recent_date]
        return None


if __name__ == "__main__":
    try:
        service = ChessRankingsService()
        service.print_top_50_classical_players()
        service.print_last_30_day_rating_for_top_player()
        service.generate_rating_csv_for_top_50_classical_players()
    except ChessRankingsServiceError as e:
        print(e)
