with defi_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        contains_substr(content, "defi")
        and not contains_substr(entity, "defi")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "defi")

)

select * from defi_tweets
union all
select * from base