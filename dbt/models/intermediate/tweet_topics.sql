with tweets as (

    select

        id
        , url
        , content
        , entity
        , created_timestamp

    from
        {{ source("marts", "fct_tweets") }}

)

, final as (

    select
    
        id
        , url
        , created_timestamp
        , array_agg(mentioned_topics ignore nulls) topics
        
    from
        tweets
        , unnest([
            if( contains_substr(content, "defi") or contains_substr(entity, "def"), "defi", null)
            , if ( contains_substr(content, "nft") or contains_substr(content, "nfts") or contains_substr(entity, "nft"), "nft", null)
            , if ( contains_substr(content, "web3") or contains_substr(entity, "web3"), "web3", null)
            , if ( contains_substr(content, "dao") or contains_substr(entity, "dao"), "dao", null)
        ]) mentioned_topics
    group by
        1, 2, 3

)

select * from final