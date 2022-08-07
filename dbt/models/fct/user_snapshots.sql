{{
    config(
        unique_key="id"
    )
}}

with users as (

    select 
        
        *
        , row_number() over(
            partition by id order by timestamp_trunc(data_ts, DAY, "UTC") desc
        ) row_number

    from 
        {{ source("raw", "user") }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where 
        data_ts > (
            select max(timestamp_trunc(data_ts, DAY, "UTC")
        ) from {{ this }})
    {% endif %}

)

select 
    * except(row_number)
from 
    users
where 
    row_number = 1

