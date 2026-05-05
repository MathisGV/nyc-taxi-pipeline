select
    t.*,
    w.weather_category,
    w.temperature,
    w.humidity,
    w.wind_speed
from {{ ref('stg_fact_taxi_trips') }} t
left join {{ ref('stg_dim_weather') }} w
    on date_trunc('hour', t.pickup_datetime) = date_trunc('hour', w.weather_timestamp)