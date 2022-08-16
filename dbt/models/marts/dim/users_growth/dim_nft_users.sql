with tweet_users as (

    select 

        id
        , user_id
        , created_timestamp
        
    from {{ ref("nft_tweets") }}

)

select * from tweet_users


