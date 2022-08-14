with erg_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "erg") or contains_substr(content, "ergo") )
        and not contains_substr(entity, "erg")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "erg")

)

select * from erg_tweets
union all
select * from base