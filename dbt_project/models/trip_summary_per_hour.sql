{{ config(materialized='table') }}

select
    pickup_hour,
    weather_category,
    count(*) as total_trips,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(fare_amount) as avg_fare_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(tip_percentage) as avg_tip_percentage,
    avg(total_amount) as avg_total_amount,
    avg(temperature) as avg_temperature,
    avg(humidity) as avg_humidity,
    avg(wind_speed) as avg_wind_speed
from {{ ref('trip_enriched') }}
group by pickup_hour, weather_category
order by pickup_hour, weather_category