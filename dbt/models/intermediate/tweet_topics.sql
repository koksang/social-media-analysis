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

, flat as (

    select
    
        distinct id
        , url
        , created_timestamp
        , topic
        
    from
        tweets
        , unnest([
            if( contains_substr(content, "defi") or contains_substr(entity, "def"), "defi", null)
            , if ( contains_substr(content, "nft") or contains_substr(content, "nfts") or contains_substr(entity, "nft"), "nft", null)
            , if ( contains_substr(content, "web3") or contains_substr(entity, "web3"), "web3", null)
            , if ( contains_substr(content, "dao") or contains_substr(entity, "dao"), "dao", null)
            , if ( contains_substr(content, "coinbase") or contains_substr(entity, "coinbase"), "coinbase", null)
            , if ( contains_substr(content, "binance") or contains_substr(entity, "binance"), "binance", null)
            , if ( contains_substr(content, "ftx") or contains_substr(entity, "ftx"), "ftx", null)
            , if ( contains_substr(content, "ark") or contains_substr(entity, "ark"), "arkinvest", null)
            , if ( contains_substr(content, "nansen ai") or contains_substr(entity, "nansen"), "nansenai", null)
        ]) topic

)

, final as (

    select
    
        id
        , url
        , created_timestamp
        , array_agg(topic ignore nulls) topics
        
    from
        flat
    group by
        1, 2, 3

)

select * from final