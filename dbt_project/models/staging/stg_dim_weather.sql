select
    *
from {{ source('raw', 'dim_weather') }}