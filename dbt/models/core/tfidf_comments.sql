-- noqa: disable=TMP
{{ config(materialized="table") }}

with
    comments as (select * from {{ ref("base_subreddit_data_raw__comments") }})

    {{ tfidf("comments", "body", false) }}
