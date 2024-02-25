with source as (

    select * from {{ source('sba', 'SBA_Data') }}
)
select 
    *,
    current_timestamp as ingestion_timestamp  
from source