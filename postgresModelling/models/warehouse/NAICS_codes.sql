with source as (

    select * from {{ source('sba', 'SBA_Sector_Description') }}
)
select 
    "LookupCodes",
    "Sector",
    current_timestamp as ingestion_timestamp  
from source