"""
Generate synthetic weather data covering all 24h x 7 days combinations
and insert into raw.dim_weather.

NYC January 2025 climate averages are used as reference.
This fills the gap left by the free OpenWeatherMap API (no historical data).
"""

import os
import random
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

load_dotenv()

SEED = 42
random.seed(SEED)

# NYC January averages
HOUR_TEMP = {
    0: -1.5, 1: -2.0, 2: -2.3, 3: -2.5, 4: -2.6, 5: -2.4,
    6: -2.0, 7: -1.2, 8: 0.0,  9: 1.5,  10: 2.8, 11: 3.8,
    12: 4.5, 13: 4.8, 14: 4.6, 15: 4.0, 16: 3.0, 17: 1.8,
    18: 0.8, 19: 0.0, 20: -0.5, 21: -1.0, 22: -1.3, 23: -1.5,
}

WEATHER_POOL = [
    ("Clear",  "clear sky",        0.45),
    ("Clouds", "overcast clouds",  0.30),
    ("Rain",   "light rain",       0.15),
    ("Snow",   "light snow",       0.07),
    ("Thunderstorm", "thunderstorm", 0.03),
]

def pick_weather():
    r = random.random()
    cumul = 0.0
    for main, desc, prob in WEATHER_POOL:
        cumul += prob
        if r <= cumul:
            return main, desc
    return WEATHER_POOL[-1][0], WEATHER_POOL[-1][1]

def weather_category(main: str) -> str:
    if main == "Clear":
        return "Clear"
    if main in ("Rain", "Drizzle"):
        return "Rainy"
    if main in ("Thunderstorm", "Snow"):
        return "Stormy"
    return "Other"

def generate_rows():
    rows = []
    base = datetime(2025, 1, 6, 0, 0, 0, tzinfo=timezone.utc)  # Monday
    for day in range(7):
        for hour in range(24):
            ts = base.replace(day=base.day + day, hour=hour)
            temp    = round(HOUR_TEMP[hour] + random.uniform(-1.5, 1.5), 2)
            feels   = round(temp - random.uniform(1.0, 3.0), 2)
            humidity = random.randint(55, 72)
            wind    = round(random.uniform(2.0, 9.0), 2)
            wind_deg = random.randint(0, 359)
            main, desc = pick_weather()
            category = weather_category(main)
            rows.append((
                ts,
                temp, feels, humidity,
                wind, wind_deg,
                main, desc, category,
                hour,
                ts.isoweekday() % 7 + 1,  # 1=Sun ... 7=Sat, matching PostgreSQL dayofweek
                datetime.now(timezone.utc),
            ))
    return rows


def main():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("PGPORT", 5432)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("DELETE FROM raw.dim_weather;")
    print("Table raw.dim_weather vidée")

    rows = generate_rows()
    cur.executemany("""
        INSERT INTO raw.dim_weather
            (recorded_at, temperature, feels_like, humidity,
             wind_speed, wind_deg, weather_main, weather_description,
             weather_category, pickup_hour, day_of_week, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"{len(rows)} lignes insérées dans raw.dim_weather")


if __name__ == "__main__":
    main()