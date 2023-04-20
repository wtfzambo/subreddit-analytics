import hashlib
import inspect
import os
import pickle
from functools import wraps
from pathlib import Path
from typing import Any, Literal, TypeVar, overload

import duckdb
from asyncpraw.models import Comment, Redditor, Submission
from asyncpraw.reddit import Reddit as AReddit
from dotenv import load_dotenv
from praw.reddit import Reddit as SReddit


load_dotenv()


class DuckDBManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls.__initialized = False
        return cls._instance

    def __init__(self, subreddit: str | None = None):
        if not self.__initialized:
            root = get_project_root()
            duckdb_path = root / f"assets/{subreddit}.duckdb"
            self.duckdb_con = duckdb.connect(duckdb_path.as_posix())
            self.__initialized = True


T = TypeVar("T")

# consts
COMMENT = "ti"
REDDITOR = "t2"
SUBMISSION = "t3"
MESSAGE = "t4"
SUBREDDIT = "t5"
AWARD = "t6"

# SQL Queries
CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {df}"
CREATE_OR_REPLACE_TABLE = "CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table} UNION BY NAME SELECT * FROM {df}"


@overload
def get_reddit_client(*, is_async: Literal[True] = True) -> AReddit:
    ...


@overload
def get_reddit_client(*, is_async: Literal[False] = False) -> SReddit:
    ...


def get_reddit_client(*, is_async: bool = False) -> AReddit | SReddit:
    user_agent = os.getenv("REDDIT_USER_AGENT")
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    Reddit = AReddit if is_async else SReddit
    return Reddit(
        user_agent=user_agent, client_id=client_id, client_secret=client_secret
    )


def cache_results(*, refresh_cache=False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # hash the input args
            input_hash = hashlib.md5(repr((args, kwargs)).encode()).hexdigest()

            module_path = Path(inspect.getfile(func)).parent
            cache_folder = module_path / ".cache"
            os.makedirs(cache_folder, exist_ok=True)

            # check if the hash matches with an existing file
            file_path = cache_folder / f"{input_hash}.pkl"
            if os.path.isfile(file_path and not refresh_cache):
                with open(file_path, "rb") as file:
                    result = pickle.load(file)
                    print("Loaded result from file: ", file_path.name)
            else:
                result = func(*args, **kwargs)
                with open(file_path, "wb") as file:
                    pickle.dump(result, file, protocol=pickle.HIGHEST_PROTOCOL)
                    print("Stored result in file: ", file_path.name)
            return result

        return wrapper

    return decorator


def chunked(iterable: list[T] | tuple[T], chunk_size: int) -> list[list[T]]:
    d = {}
    for i, x in enumerate(iterable):
        d.setdefault(i // chunk_size, []).append(x)
    return list(d.values())


def get_project_root():
    return Path(__file__).parent.parent


def _isPrimitive(obj):
    return not hasattr(obj, "__dict__")


def _no_empty_obj(v):
    if not isinstance(v, (list, dict)):
        return v
    return None if len(v) == 0 else v


def clean_up_reddit_object(d: dict):
    return {k: _no_empty_obj(v) for k, v in d.items() if _isPrimitive(v)}


def replace_author_object_with_name(sub_or_comment_dict: dict[str, Any]):
    return (
        sub_or_comment_dict | {"author": sub_or_comment_dict["author"].name}
        if isinstance(sub_or_comment_dict["author"], Redditor)
        else sub_or_comment_dict
    )


def clean_entries(entries: list[Submission] | list[Comment]):
    entries_as_dict = [vars(sub) for sub in entries]
    entries_with_author = list(map(replace_author_object_with_name, entries_as_dict))
    return list(map(clean_up_reddit_object, entries_with_author))
