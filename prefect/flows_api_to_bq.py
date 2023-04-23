import argparse
import asyncio
from datetime import date, timedelta
from typing import cast

from asyncpraw.models import Comment, Submission
from tasks_get_subreddit_data import (
    add_records_to_duckdb,
    get_submission_comments,
    get_submission_ids_but_im_cheating,
    get_submissions_from_ids,
)
from utils_ import AsyncRedditManger, DuckDBManager, chunked, clean_entries

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner


@flow(
    name="Get subreddit data",
    flow_run_name="r/{subreddit} | from '{start_date}' to '{end_date}'",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(),
)
async def get_subreddit_data(start_date: str, end_date: str, subreddit: str):
    # submission_ids = get_submission_ids(start_date, end_date, subreddit)
    submission_ids = get_submission_ids_but_im_cheating()
    submission_ids_chunked = chunked(submission_ids, 100)

    print("Getting all submissions...")
    submission_futures = get_submissions_from_ids.map(
        cast(list[str], submission_ids_chunked)
    )
    submissions_all: list[Submission] = []
    for future in submission_futures:
        submissions = [submission async for submission in future.result()]
        submissions_all.extend(submissions)

    print("Getting all submissions' comments...")
    submission_comments_futures = get_submission_comments.map(
        cast(Submission, submissions_all)
    )
    comments_all: list[Comment] = []
    for future in submission_comments_futures:
        comments = [comment async for comment in future.result()]
        comments_all.extend(comments)

    print("Closing reddit sessions...")
    reddit_manager = AsyncRedditManger()
    for instance in reddit_manager.reddit_instances:
        await instance.close()

    submissions_clean = clean_entries(submissions_all)
    comments_clean = clean_entries(comments_all)

    print("Adding submissions and comments to duckdb...")
    # create duckdb connection
    DuckDBManager(subreddit)
    add_records_to_duckdb(submissions_clean, "submissions")
    add_records_to_duckdb(comments_clean, "comments")


async def main():
    parser = argparse.ArgumentParser(
        description="Get subreddit data in a given time range."
    )
    parser.add_argument("-s", "--start", help="Start date in y-m-d format", type=str)
    parser.add_argument("-e", "--end", help="End date in y-m-d format", type=str)
    parser.add_argument("-r", "--subreddit", help="Subreddit name", type=str)

    args = parser.parse_args()

    start_date = args.start or (date.today() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = args.end or date.today().strftime("%Y-%m-%d")
    subreddit = args.subreddit or "dataengineering"

    return await get_subreddit_data(start_date, end_date, subreddit)


if __name__ == "__main__":
    asyncio.run(main())
