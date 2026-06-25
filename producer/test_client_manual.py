from dotenv import load_dotenv

load_dotenv()

from config import FootballAPIConfig
from api_client import FootballDataClient

if __name__ == "__main__":
    config = FootballAPIConfig()

    with FootballDataClient(config) as client:
        print("-> Pobieram mecze Premier League (zaplanowane)...")
        matches = client.get_matches_by_competition("PL", status="SCHEDULED")
        print(f"Pobrano {len(matches)} meczy")

        if matches:
            first = matches[0]
            print(f" Przykład: {first['homeTeam']['name']} vs {first['awayTeam']['name']}")
            print(f" Data: {first['utcDate']}")
        
        print("/n-> Sprawdzam mecze live...")
        live = client.get_live_matches()
        print(f" Mecze live: {len(live)}")