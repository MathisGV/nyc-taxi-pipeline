select
    passenger_count,
    count(*) as total_trips,
    sum(total_amount) as total_spent,
    avg(tip_percentage) as avg_tip_percentage
from {{ ref('trip_enriched') }}
group by passenger_count
having count(*) > 10
   and sum(total_amount) > 300
   and avg(tip_percentage) > 0.15