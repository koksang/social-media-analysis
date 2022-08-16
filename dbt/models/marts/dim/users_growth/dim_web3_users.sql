with tweet_users as (

    select 

        id
        , user_id
        , created_timestamp
        
    from {{ ref("web3_tweets") }}

)

select * from tweet_users


