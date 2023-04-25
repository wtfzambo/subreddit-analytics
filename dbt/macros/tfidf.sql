{%- macro tfidf(relation, text_field, id_column_name) -%}

    ,

    doc_words as (
        select
            id,
            regexp_extract_all(
                lower(regexp_replace({{ text_field }}, r'<[^>]+>', '')), '[a-z]{2,}'
            ) as words_array,
            count(*) over () as n_docs
        from {{ relation }}
        where
            not regexp_contains(
                lower({{ text_field }}), r'(\[deleted])|(\[removed])|(\[view poll])'
            )
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
        having words_in_doc > 30
    ),

    words_tf_per_doc as (
        select
            word,
            array_agg(struct(tf, id, words_in_doc)) as tfs,
            any_value(n_docs) as n_docs
        from words_tf
        group by 1
    ),

    docs_idf as (
        select
            tf.id as {{ id_column_name }},
            word,
            tf.tf,
            array_length(tfs) as docs_with_word,
            log(n_docs / array_length(tfs)) as idf
        from words_tf_per_doc
        cross join unnest(tfs) as tf
    ),

    final as (select *, tf * idf as tfidf from docs_idf)

    select *
    from final {{ remove_stopwords("word") }}

{%- endmacro -%}
