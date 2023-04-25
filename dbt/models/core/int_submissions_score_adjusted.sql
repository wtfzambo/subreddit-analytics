with

    submissions as (select * from {{ ref("base_subreddit_data_raw__submissions") }}),

    quantiles as (
        select
            approx_quantiles(distinct upvotes, 100)[offset(50)] as upvotes_q50,
            approx_quantiles(distinct num_comments, 100)[offset(50)] as n_comments_q50
        from submissions
    ),

    cap_upvotes as (
        select
            s.*,
            if(s.upvotes >= q.upvotes_q50, q.upvotes_q50, s.upvotes) as upvotes_capped,
            if(
                s.num_comments >= q.n_comments_q50, q.n_comments_q50, s.num_comments
            ) as n_comments_capped
        from submissions as s
        cross join quantiles as q
    ),

    rescaled as (
        select
            *,
            ml.robust_scaler(upvotes_capped) over () as upvotes_rescaled,
            ml.robust_scaler(n_comments_capped) over () as n_comments_rescaled
        from cap_upvotes
    ),

    minmaxed as (
        select
            *,
            ml.min_max_scaler(upvotes_rescaled) over () as upvotes_minmax,
            ml.min_max_scaler(n_comments_rescaled) over () as n_comments_minmax
        from rescaled
    ),

    final as (
        select *, (upvotes_minmax + n_comments_minmax) / 2 as score_adjusted
        from minmaxed
    )

select *
from final
