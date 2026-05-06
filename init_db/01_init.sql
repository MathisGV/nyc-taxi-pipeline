CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.fact_taxi_trips (
    vendor_id               INTEGER,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         FLOAT,
    trip_distance           FLOAT,
    rate_code_id            FLOAT,
    store_and_fwd_flag      VARCHAR(1),
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            INTEGER,
    fare_amount             FLOAT,
    extra                   FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    total_amount            FLOAT,
    congestion_surcharge    FLOAT,
    cbd_congestion_fee      FLOAT,
    trip_duration_minutes   FLOAT,
    distance_bucket         VARCHAR(10),
    payment_label           VARCHAR(50),
    tip_percentage          FLOAT,
    pickup_hour             INTEGER,
    pickup_day_of_week      INTEGER,
    ingested_at             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.dim_weather (
    weather_id          SERIAL PRIMARY KEY,
    recorded_at         TIMESTAMP,
    temperature         FLOAT,
    feels_like          FLOAT,
    humidity            INTEGER,
    wind_speed          FLOAT,
    wind_deg            INTEGER,
    weather_main        VARCHAR(50),
    weather_description VARCHAR(100),
    weather_category    VARCHAR(20),
    pickup_hour         INTEGER,
    day_of_week         INTEGER,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_taxi_pickup_datetime
    ON raw.fact_taxi_trips (tpep_pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_taxi_pickup_hour
    ON raw.fact_taxi_trips (pickup_hour);

CREATE INDEX IF NOT EXISTS idx_weather_recorded_at
    ON raw.dim_weather (recorded_at);

CREATE INDEX IF NOT EXISTS idx_weather_pickup_hour
    ON raw.dim_weather (pickup_hour);