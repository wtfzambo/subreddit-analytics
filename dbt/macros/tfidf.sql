{%- macro tfidf(relation, text_field, is_submission) -%}

    ,

    doc_words as (
        select
            id,
            regexp_extract_all(
                lower(regexp_replace({{ text_field }}, r'<[^>]+>', '')), '[a-z]{2, }'
            ) as words_array,
            count(*) over () as n_docs
        from {{ relation }}
        where
            not regexp_contains(
                lower({{ text_field }}),
                r'(\[deleted])|(\[removed])|(\[view poll])|(\[http.+])'
            )
            {% if is_submission -%} and is_self = true {%- endif %}
    ),

    words_tf as (
        select
            id,
            word,
            count(*) / array_length(any_value(words_array)) as tf,
            array_length(any_value(words_array)) as words_in_doc,
            any_value(n_docs) as n_docs
        from doc_words
        cross join unnest(words_array) as word
        group by 1, 2
        having words_in_doc > 20
    )

    select *
    from words_tf

{%- endmacro -%}
