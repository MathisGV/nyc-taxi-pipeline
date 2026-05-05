select
    *
from {{ source('raw', 'fact_taxi_trips') }}