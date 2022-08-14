with vitalik_buterin_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "vitalik") or contains_substr(content, "vitalik buterin") )
        and not contains_substr(entity, "vitalik")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "vitalik")

)

select * from vitalik_buterin_tweets
union all
select * from base