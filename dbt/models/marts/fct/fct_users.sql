{{
    config(
        unique_key=["id", "data_date"]
    )
}}

with users as (

    select 
        
        *
        , row_number() over(
            partition by id, timestamp_trunc(data_ts, DAY, "UTC")
            order by data_ts desc
        ) row

    from 
        {{ source("raw", "user") }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where 
        data_ts > ( 
            select max(timestamp_trunc(data_ts, DAY, "UTC")) from {{ this }}
        )
    {% endif %}
    
)

select 
    * except(row)
    , extract(date from data_ts) data_date
from 
    users
where
    row = 1

