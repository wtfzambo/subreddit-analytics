with
    submissions as (select * from {{ ref("base_subreddit_data_raw__submissions") }}),

    comments as (select * from {{ ref("base_subreddit_data_raw__comments") }}),

    all_authors as (
        select distinct author_id, author_name
        from submissions

        union
        distinct

        select distinct author_id, author_name
        from comments
    ),

    final as (
        select author_id, to_hex(sha1(author_name)) as author_hash from all_authors
    )

select *
from final
where author_id is not null
