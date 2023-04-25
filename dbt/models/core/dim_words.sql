with

    tfidf as (select * from {{ ref("tfidf_submissions_and_comments") }}),

    submissions as (select * from {{ ref("int_submissions_score_adjusted") }}),

    words_score_adjusted as (
        select
            tf.word,
            tf.docs_with_word,
            tf.tfidf * s.score_adjusted * tf.docs_with_word as word_score
        from tfidf as tf
        left join submissions as s on tf.submission_id = s.id
    ),

    final as (
        select
            word,
            any_value(docs_with_word) as docs_with_word,
            round(avg(word_score), 3) as word_score
        from words_score_adjusted
        group by 1
    )

select *
from final
