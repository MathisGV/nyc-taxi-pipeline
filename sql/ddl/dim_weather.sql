CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id BIGSERIAL PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    city VARCHAR(100) NOT NULL,
    temperature_c NUMERIC(6, 2),
    humidity_pct INTEGER,
    wind_speed_ms NUMERIC(6, 2),
    weather_main VARCHAR(80),
    weather_description VARCHAR(255),
    weather_category VARCHAR(20),
    hour_of_day SMALLINT,
    day_of_week SMALLINT,
    source_file VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_humidity_pct CHECK (humidity_pct IS NULL OR humidity_pct BETWEEN 0 AND 100),
    CONSTRAINT chk_hour_of_day CHECK (hour_of_day IS NULL OR hour_of_day BETWEEN 0 AND 23),
    CONSTRAINT chk_day_of_week CHECK (day_of_week IS NULL OR day_of_week BETWEEN 0 AND 6),
    CONSTRAINT chk_weather_category CHECK (
        weather_category IS NULL OR weather_category IN ('Clair', 'Pluvieux', 'Orageux', 'Autre')
    )
);

CREATE INDEX IF NOT EXISTS idx_dim_weather_event_ts
    ON dim_weather (event_ts);

CREATE INDEX IF NOT EXISTS idx_dim_weather_weather_category
    ON dim_weather (weather_category);

CREATE INDEX IF NOT EXISTS idx_dim_weather_city_event_ts
    ON dim_weather (city, event_ts);
