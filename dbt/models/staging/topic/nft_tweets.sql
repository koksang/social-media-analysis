with btc_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "nft") or contains_substr(content, "nfts") )
        and not contains_substr(entity, "nft")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "nft")

)

select * from btc_tweets
union all
select * from base