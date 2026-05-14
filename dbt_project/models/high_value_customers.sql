{{ config(materialized='table') }}

select
    pu_location_id,
    count(*)              as total_trips,
    sum(total_amount)     as total_spent,
    avg(tip_percentage)   as avg_tip_percentage,
    avg(trip_distance)    as avg_trip_distance,
    avg(total_amount)     as avg_ticket
from {{ ref('trip_enriched') }}
group by pu_location_id
having
    count(*)          > 10
    and sum(total_amount)   > 300
    and avg(tip_percentage) > 15
order by total_spent desc