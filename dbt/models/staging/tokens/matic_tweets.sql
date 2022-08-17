with tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where
        contains_substr(content, "matic")
        and not contains_substr(entity, "matic")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "matic")

)

, final as (

    select * from tweets
    union all
    select * from base

)

select
    
    *
    , 'token' tweet_type
    , 'matic' tweet_type_value
    
from
    final