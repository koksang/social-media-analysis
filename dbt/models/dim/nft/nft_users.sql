with tweet_users as (

    select id, user_id
    from {{ ref("nft_tweets") }}

)

, users_base as (

    select id, data_ts  
    from {{ source("fct", "user_snapshots") }}

)

select
    a.id as user_id
    , extract(week from b.data_ts) week 
from 
    tweet_users a
left join
    users_base b
on
    a.user_id = b.id


