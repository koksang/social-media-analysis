with tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "ada") or contains_substr(content, "cardano") )
        and not contains_substr(entity, "ada")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "ada")

)

, final as (

    select * from tweets
    union all
    select * from base

)

select
    
    *
    , 'token' tweet_type
    , 'ada' tweet_type_value
    
from
    final