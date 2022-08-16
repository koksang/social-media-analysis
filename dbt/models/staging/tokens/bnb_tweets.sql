with tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        regexp_contains(lower(content), r"(bnb|binance coin|binance smart chain|bsc)")
        and not contains_substr(entity, "bnb")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "bnb")

)

, final as (

    select * from ada_tweets
    union all
    select * from base

)

select
    
    *
    , 'token' tweet_type
    , 'bnb' tweet_type_value
    
from
    final