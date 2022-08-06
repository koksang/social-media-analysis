{{
    config(
        unique_key="id",
        partition_key={ 
            "field": "data_ts", "data_type": "timestamp", "granularity": "month" 
        }
    )
}}

with users as (

    select 
        
        *
        , rank() over(
            partition by id order by timestamp_trunc(data_ts, DAY, "UTC") desc
        ) rank

    from 
        {{ source("raw", "users") }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where 
        data_ts > (
            select max(timestamp_trunc(data_ts, DAY, "UTC")
        ) from {{ this }})
    {% endif %}

)

select 
    * except(rank)
from 
    users
where 
    rank = 1

