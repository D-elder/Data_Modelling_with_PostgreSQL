with source as (

    select * from {{ source('sba', 'SBA_Data') }}
)
select *from source