with tweet_tokens as (

    select

        (tokens is not null) has_tokens
        , count(distinct id) tweets_count

    from 
        {{ ref("tweet_tokens") }}
    group by
        1

)

select * from tweet_tokens