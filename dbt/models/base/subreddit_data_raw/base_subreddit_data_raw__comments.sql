with source as (
    select * from {{ source('subreddit_data_raw', 'comments') }}
)

, renamed as (
    select
        name as id
        , archived
        , author_fullname as author_id
        , author as author_name
        , author_flair_text
        , body
        , timestamp_seconds(created_utc) as created_at_utc
        , date(timestamp_seconds(created_utc)) as creation_date
        , distinguished
        , is_submitter
        , link_id as submission_id
        , parent_id
        , 'https://reddit.com' || permalink as url
        , ups as upvotes

    from source
)

select * from renamed
