with source as (
    select * from {{ source('subreddit_data_raw', 'submissions') }}
)

, renamed as (
    select
        name as id
        , archived
        , author_fullname as author_id
        , author as author_name
        , author_flair_text
        , timestamp_seconds(created_utc) as created_at_utc
        , date(timestamp_seconds(created_utc)) as creation_date
        , domain
        , is_self
        , link_flair_text
        , link_flair_type
        , num_comments
        , num_crossposts
        , 'https://reddit.com' || permalink as submission_url
        , ups as upvotes
        , upvote_ratio
        , selftext
        , subreddit_subscribers
        , title
        , url
    from source
)

select * from renamed
