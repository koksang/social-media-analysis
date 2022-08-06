with btc_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "btc") or contains_substr(content, "bitcoin") )
        and entity not in ("btc", "bitcoin")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("btc", "bitcoin")

)

select * from btc_tweets
union all
select * from base