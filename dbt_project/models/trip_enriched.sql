{{ config(materialized='table') }}

with taxi as (
    select
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_hour,
        pickup_day_of_week,
        fare_amount,
        tip_amount,
        tip_percentage,
        total_amount,
        payment_label,
        distance_bucket,
        trip_duration_minutes,
        pu_location_id,
        do_location_id
    from {{ source('raw', 'fact_taxi_trips') }}
),

weather as (
    select
        pickup_hour,
        temperature,
        feels_like,
        humidity,
        wind_speed,
        weather_main,
        weather_description,
        weather_category
    from {{ source('raw', 'dim_weather') }}
),

enriched as (
    select
        t.tpep_pickup_datetime,
        t.tpep_dropoff_datetime,
        t.passenger_count,
        t.trip_distance,
        t.distance_bucket,
        t.trip_duration_minutes,
        t.pickup_hour,
        t.pickup_day_of_week,
        t.pu_location_id,
        t.do_location_id,
        t.fare_amount,
        t.tip_amount,
        t.tip_percentage,
        t.total_amount,
        t.payment_label,
        w.temperature,
        w.feels_like,
        w.humidity,
        w.wind_speed,
        w.weather_main,
        w.weather_description,
        w.weather_category
    from taxi t
    left join weather w
        on t.pickup_hour = w.pickup_hour
)

select * from enriched