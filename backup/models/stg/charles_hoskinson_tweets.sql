with charles_hoskinson_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "charles hoskinson") or contains_substr(content, "charles") or contains_substr(content, "hoskinson") )
        and not contains_substr(entity, "charles hoskinson")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "charles hoskinson")

)

select * from charles_hoskinson_tweets
union all
select * from base