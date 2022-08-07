with cz_peng_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "cz peng") or contains_substr(content, "cz") 
        or contains_substr(content, "czbinance") or contains_substr(content, "cz_binance") )
        and not contains_substr(entity, "cz peng")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "cz peng")

)

select * from cz_peng_tweets
union all
select * from base