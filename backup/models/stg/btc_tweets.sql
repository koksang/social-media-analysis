with btc_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "btc") or contains_substr(content, "bitcoin") )
        and not contains_substr(entity, "btc")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "btc")

)

select * from btc_tweets
union all
select * from base