-- noqa: disable=TMP
with

    submissions as (
        select *
        from {{ ref("base_subreddit_data_raw__submissions") }}
        where
            not regexp_contains(
                lower(selftext), r'(\[deleted])|(\[removed])|(\[view poll])'
            )
            and is_self = true
    ),

    comments as (
        select *
        from {{ ref("base_subreddit_data_raw__comments") }}
        where
            not regexp_contains(
                lower(body), r'(\[deleted])|(\[removed])|(\*i am a bot)'
            )
    ),

    comments_body_grouped_by_submission as (
        select submission_id, array_to_string(array_agg(body), ' ') as body
        from comments
        group by 1
    ),

    submissions_and_comments as (
        select s.id, s.title || ' ' || s.selftext || ' ' || c.body as body
        from submissions as s
        left join comments_body_grouped_by_submission as c on s.id = c.submission_id
    )

    {{ tfidf("submissions_and_comments", "body", "submission_id") }}
