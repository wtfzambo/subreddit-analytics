import asyncio
from datetime import datetime, timedelta
from typing import AsyncGenerator, cast

import pandas as pd
from asyncpraw.models import Submission
from pmaw import PushshiftAPI
from utils_ import (
    SUBMISSION,
    cache_results,
    chunked,
    get_project_root,
    get_reddit_client,
)

from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.tasks import task_input_hash


@cache_results(refresh_cache=True)
def get_submission_ids(start_date: str, end_date: str, subreddit: str):
    # For some fucking reason, decorating this function with @task makes it run not in
    # the main thread, resulting in the following error message:
    # `ValueError: signal only works in main thread of the main interpreter`
    # So, since the execution can be pretty slow, we wrap it in a custom caching decorator
    reddit = get_reddit_client(is_async=False)
    api_praw = PushshiftAPI(praw=reddit)
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    start_date_ts = int(start_datetime.timestamp())
    end_date_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    search_window_days = (datetime.today() - start_datetime).days

    print(f"Searching for submissions between {start_date} and {end_date}")

    submissions = api_praw.search_submissions(
        subreddit=subreddit,
        after=start_date_ts,
        until=end_date_ts,
        search_window=search_window_days,
    )

    return [f'{SUBMISSION}_{sub["id"]}' for sub in submissions]


@task(log_prints=True, name="Get submission ids from CSV")
def get_submission_ids_but_im_cheating():
    # Fucking PushshiftAPI seems to have gone down while I was working on this project.
    # Luckily I had saved about a year of submission ids in a csv a week before, so I'm
    # going to use that instead.
    root = get_project_root()
    COLS = ["post_id"]
    df = pd.read_csv(root / "assets/post_ids.csv", header=None, names=COLS)
    post_ids = df[COLS[0]].values
    print(f"Found {len(post_ids)} submissions")
    return [f"{SUBMISSION}_{post_id}" for post_id in post_ids]


@task(name="Get submission from ids")
async def get_submission_from_ids(ids: list[str]):
    with get_reddit_client(is_async=True) as reddit:
        subs: AsyncGenerator[Submission, None] = reddit.info(fullnames=ids)  # type: ignore
        submissions = [sub async for sub in subs]
        return submissions


@flow(
    log_prints=True,
    name="Get subreddit data",
    flow_run_name="r/{subreddit} | from '{start_date}' to '{end_date}'",
    task_runner=ConcurrentTaskRunner(),
)
async def get_subreddit_data(start_date: str, end_date: str, subreddit: str):
    # submission_ids = get_submission_ids(start_date, end_date, subreddit)
    submission_ids = get_submission_ids_but_im_cheating()

    submission_ids_chunked = chunked(submission_ids, 100)
    submission_futures = await get_submission_from_ids.map(
        cast(list[str], submission_ids_chunked)
    )

    submissions: list[Submission] = []
    for future in submission_futures:
        submissions.extend(await future.result())

    print(submissions)


async def main():
    return await get_subreddit_data("2023-04-14", "2023-04-15", "dataengineering")


if __name__ == "__main__":
    asyncio.run(main())


# 3. from the Submission list, 3 things must be done:
#   - Build the submission table - Remember that `author` is an object, not a string.
#     So it needs to be replaced using `Author.name`.`
#   - Add Submission author to the author's table (TBD when we're doing this).
#   - Parse Submission comments
#
# For comments, there are 3 possibilities:
# - No comments -> Skip (get this from actual comment list, not from property `num_comments`)
# - Comments without Load more -> Append to comments table
# - Comments WITH Load more -> run `submission.comments.replace_more(limit=None)` ->
#   -> Append to comments table
# For both 2nd and 3rd case, also append author to authors table.
#
# At a high level, flow could be something like this:
# 1 - Get submission IDs
# 2 - From IDs get submission objects
# 3 - From Submission objects build submission table
# 4 - Parse each Submission object for comments
#
# Common operations between submissions and comments:
# - Convert from object to dict
# - Keep only primitive variables
# - Replace author object with author's name
# - Somehow extract author and append to authors table. NOTE: This could be done at the end using
#   duckdb tables directly and `INSERT INTO ON CONFLICT` statement so that authors can be
#   already unique.
# - Append to duckdb table
# - Eventually recast unix time columns to INT
