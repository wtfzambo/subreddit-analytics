-- noqa: disable=TMP
{{ config(materialized="table") }}

with
    submissions as (select * from {{ ref("base_subreddit_data_raw__submissions") }})

    {{ tfidf("submissions", "selftext", true) }}
