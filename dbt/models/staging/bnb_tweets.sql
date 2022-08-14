with bnb_tweets as (

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

select * from bnb_tweets
union all
select * from base