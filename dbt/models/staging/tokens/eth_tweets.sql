with tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "eth") or contains_substr(content, "ethereum") )
        and not contains_substr(entity, "eth")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "eth")

)

, final as (

    select * from tweets
    union all
    select * from base

)

select
    
    *
    , 'token' tweet_type
    , 'eth' tweet_type_value
    
from
    final