with elon_musk_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "elon musk") or contains_substr(content, "elon") )
        and not contains_substr(entity, "elon")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "elon")

)

select * from elon_musk_tweets
union all
select * from base