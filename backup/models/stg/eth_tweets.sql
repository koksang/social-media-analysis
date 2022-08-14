with eth_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "eth") or contains_substr(content, "ethereum") )
        and not contains_substr(entity, "eth")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "eth")

)

select * from eth_tweets
union all
select * from base