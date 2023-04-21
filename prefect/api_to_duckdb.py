import argparse
import asyncio
from datetime import date, datetime, timedelta
from typing import Any, AsyncGenerator, Coroutine, Literal, cast

import pandas as pd
from asyncpraw.models import Comment, Submission
from pmaw import PushshiftAPI
from utils_ import (
    CREATE_TABLE_IF_NOT_EXISTS,
    INSERT_OR_REPLACE_INTO,
    SCHEMAS,
    SUBMISSION,
    AsyncRedditManger,
    DuckDBManager,
    cache_results,
    chunked,
    clean_entries,
    get_project_root,
    get_reddit_client,
    get_schema_string,
)

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.tasks import task_input_hash


@cache_results()
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
    # PushshiftAPI seems to have gone down while I was working on this project.
    # Luckily I had saved about a year of submission ids in a csv a week before, so I'm
    # going to use that instead.
    root = get_project_root()
    COLS = ["post_id"]
    df = pd.read_csv(root / "assets/post_ids.csv", header=None, names=COLS)
    post_ids = df[COLS[0]].values
    print(f"Found {len(post_ids)} submissions")
    return [f"{SUBMISSION}_{post_id}" for post_id in post_ids]


@task(name="Get submissions from ids", log_prints=True)
async def get_submissions_from_ids(ids: list[str]):
    reddit_manager = AsyncRedditManger()
    reddit = reddit_manager.get_new_async_reddit()
    subs: AsyncGenerator[Submission, None] = reddit.info(fullnames=ids)  # type: ignore
    submissions = [sub async for sub in subs]

    coro = map(
        lambda sub: (sub.load(), print(f"Loading submission {sub.id}"))[0],
        submissions,
    )
    _ = await asyncio.gather(*coro)

    for sub in submissions:
        yield sub


@task(name="Get submission comments")
async def get_submission_comments(
    submission: Submission,
) -> AsyncGenerator[Comment, None]:
    await submission.comments.replace_more(limit=None)
    comments_list = submission.comments.list()

    if isinstance(comments_list, Coroutine):
        comments_list = await comments_list

    for comment in comments_list:
        yield comment


@task(
    name="Add records to Duckdb",
    task_run_name="Add {table} to Duckdb",
    log_prints=True,
    cache_key_fn=task_input_hash,
)
def add_records_to_duckdb(
    records: list[dict[str, Any]], table: Literal["submissions"] | Literal["comments"]
):
    if not len(records):
        return

    table_schema = SCHEMAS[table]
    fields = list(table_schema.keys())

    df = pd.DataFrame.from_records(records)
    df = df[fields]  # Need to sort the df columns in the same order as the schema
    print(f"Found {len(df)} records for {table}, adding them to Duckdb")

    schema_string = get_schema_string(table_schema)

    con = DuckDBManager().duckdb_con
    con.sql(CREATE_TABLE_IF_NOT_EXISTS.format(table=table, schema=schema_string))
    con.sql(INSERT_OR_REPLACE_INTO.format(table=table, from_="df"))


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
