with web3_tweets as (

    select 
        * except(entity)
    from 
        {{ ref("tweets") }}
    where 
        ( contains_substr(content, "web3") )
        and entity not in ("web3")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("web3")

)

select * from web3_tweets
union all
select * from base