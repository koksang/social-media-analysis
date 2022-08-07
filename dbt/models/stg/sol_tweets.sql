with sol_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "sol") or contains_substr(content, "solana") )
        and not contains_substr(entity, "matic")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "sol")

)

select * from sol_tweets
union all
select * from base 