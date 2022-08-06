with sol_tweets as (

    select 
        * except(entity)
    from 
        {{ ref("tweets") }}
    where 
        ( contains_substr(content, "sol") or contains_substr(content, "solana") )
        and entity not in ("sol", "solana")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("sol", "solana")

)

select * from sol_tweets
union all
select * from base