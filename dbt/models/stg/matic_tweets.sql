with matic_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where
        contains_substr(content, "matic")
        and not contains_substr(entity, "matic")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "matic")

)

select * from matic_tweets
union all
select * from base