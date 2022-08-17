with tweets as (

    select

       extract(week from created_timestamp) week
       , count(distinct id) tweets_count

    from 
        {{ ref("tweet_tokens") }}
    where
        created_timestamp >= "2022-05-01"
    group by
        1

)

select * from tweets 