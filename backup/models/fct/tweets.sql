{{
    config(
        unique_key = "url"
    )
}}

with tweet as (

    select 
        
        *
        , row_number() over(
            partition by url order by timestamp_trunc(created_timestamp, DAY, "UTC") desc
        ) row_number

    from 
        {{ source("raw", "tweet") }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where 
        created_timestamp > (
            select max(timestamp_trunc(created_timestamp, DAY, "UTC")
        ) from {{ this }})
    {% endif %}

)

select 
    * except(row_number)
from 
    tweet
where 
    row_number = 1

