{{
    config(
        unique_key = ["id", "entity"]
    )
}}

with tweet as (

    select 
        
        *
        , row_number() over(
            partition by id, url, entity 
            order by created_timestamp desc
        ) row

    from 
        {{ source("raw", "tweet") }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where 
        created_timestamp > ( select max(created_timestamp) from {{ this }} )
    {% endif %}

)

select 
    * except(row) 
from 
    tweet
where 
    row = 1

