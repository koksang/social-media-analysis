with tweet_users as (

    select id, user_id
    from {{ ref("web3_tweets") }}

)

, users_base as (

    select id, data_ts  
    from {{ source("marts", "fct_users") }}

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


