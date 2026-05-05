select
    date_trunc('hour', pickup_datetime) as pickup_hour,
    weather_category,
    count(*) as trip_count,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(tip_percentage) as avg_tip_percentage
from {{ ref('trip_enriched') }}
group by 1, 2