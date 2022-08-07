with ada_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "ada") or contains_substr(content, "cardano") )
        and not contains_substr(entity, "ada")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "ada")

)

select * from ada_tweets
union all
select * from base