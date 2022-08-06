with vitalik_buterin_tweets as (

    select 
        * except(entity)
    from 
        {{ ref("tweets") }}
    where 
        ( contains_substr(content, "vitalik") or contains_substr(content, "vitalik buterin") )
        and entity not in ('"vitalik buterin"')

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ('"vitalik buterin"')

)

select * from vitalik_buterin_tweets
union all
select * from base