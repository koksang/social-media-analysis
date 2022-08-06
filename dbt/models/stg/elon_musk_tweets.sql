with elon_musk_tweets as (

    select 
        * except(entity)
    from 
        {{ ref("tweets") }}
    where 
        ( contains_substr(content, "elon musk") or contains_substr(content, "elon") )
        and entity not in ('"elon musk"')

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ('"elon musk"')

)

select * from elon_musk_tweets
union all
select * from base