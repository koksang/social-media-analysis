{{
    config(
        unique_key = "url"
    )
}}

with tweet as (

    select 
        
        *
        , rank() over(
            partition by url order by timestamp_trunc(created_timestamp, DAY, "UTC") desc
        ) rank

    from 
        {{ source("raw", "tweets") }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where 
        created_timestamp > (
            select max(timestamp_trunc(created_timestamp, DAY, "UTC")
        ) from {{ this }})
    {% endif %}

)

select 
    * except(rank)
from 
    tweet
where 
    rank = 1

