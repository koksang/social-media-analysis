with tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "erg") or contains_substr(content, "ergo") )
        and not contains_substr(entity, "erg")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "erg")

)

, final as (

    select * from tweets
    union all
    select * from base

)

select
    
    *
    , 'token' tweet_type
    , 'erg' tweet_type_value
    
from
    final