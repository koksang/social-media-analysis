with bnb_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        regexp_contains(lower(content), r"\/(binance|bnb|binance coin|binance smart chain|bsc)\/")
        and entity not in ("bnb", '"binance coin"', '"binance smart chain"', "bsc", "binance")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("bnb", '"binance coin"', '"binance smart chain"', "bsc", "binance")

)

select * from bnb_tweets
union all
select * from base