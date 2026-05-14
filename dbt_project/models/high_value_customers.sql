{{ config(materialized='table') }}

select
    pickup_hour,
    pickup_day_of_week,
    payment_label,
    distance_bucket,
    weather_category,
    count(*) as total_trips,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(total_amount) as avg_total_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(tip_percentage) as avg_tip_percentage,
    sum(total_amount) as total_revenue
from {{ ref('trip_enriched') }}
where total_amount >= 30
group by
    pickup_hour,
    pickup_day_of_week,
    payment_label,
    distance_bucket,
    weather_category
order by total_revenue desc