with tweets as (

    select 
    
        id
        , url
        , created_timestamp

    from {{ ref("elon_musk_tweets") }}

)

, tweet_token_interests as (

    select 
    
        id
        , url
        , * except(id, url, created_timestamp)

    from 
        {{ ref("tweet_token_interests") }}

)

, joined as (
    select

        extract(week from a.created_timestamp) week
        , b.*
        
    from
        tweets a
    left join
        tweet_token_interests b
    on
        a.id = b.id
        and a.url = b.url
)

select * from joined