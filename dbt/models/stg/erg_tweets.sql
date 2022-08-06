with erg_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        ( contains_substr(content, "erg") or contains_substr(content, "ergo") )
        and entity not in ("erg", "ergo")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("erg", "ergo")

)

select * from erg_tweets
union all
select * from base