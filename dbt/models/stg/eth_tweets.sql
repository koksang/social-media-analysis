with eth_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "eth") or contains_substr(content, "ethereum") )
        and entity not in ("eth", "ethereum")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("eth", "ethereum")

)

select * from eth_tweets
union all
select * from base