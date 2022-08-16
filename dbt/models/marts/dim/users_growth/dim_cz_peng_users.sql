with tweet_users as (

    select

        id
        , user_id
        , created_timestamp

    from {{ ref("cz_peng_tweets") }}

)

select * from tweet_users


