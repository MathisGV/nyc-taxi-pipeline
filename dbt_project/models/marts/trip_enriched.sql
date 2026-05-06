select
    t.*,
    w.weather_category,
    w.temperature
from {{ ref('stg_fact_taxi_trips') }} t
left join {{ ref('stg_dim_weather') }} w
    on t.pickup_hour = w.pickup_hour