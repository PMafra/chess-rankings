# Chess Rankings

This project implements a service to fetch chess player rankings and rating histories from the [Lichess API](https://lichess.org/api). The `ChessRankingsService` fetches the top classical chess players, retrieves their rating histories over the last 30 days, and generates a CSV file with this information.

## Key Features

- Fetches the top 50 classical chess players from Lichess.
- Retrieves rating histories for each player over the last 30 days.
- Generates a CSV file with the player's username, rating 30 days ago, and rating today.
- Uses concurrent network requests to fetch data efficiently, reducing the overall runtime.

## Requirements

- **Python Version**: This project requires **Python 3.8** or higher.

- **Dependencies**:
  - `requests`

You can install the dependencies using `pip`:

```bash
pip install -r requirements.txt
