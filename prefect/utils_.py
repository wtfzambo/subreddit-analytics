import hashlib
import inspect
import os
import pickle
from functools import wraps
from pathlib import Path
from typing import Any, Literal, TypeVar, overload

from asyncpraw.reddit import Reddit as AReddit
from dotenv import load_dotenv
from praw.reddit import Reddit as SReddit


load_dotenv()

T = TypeVar("T")

# consts
COMMENT = "ti"
REDDITOR = "t2"
SUBMISSION = "t3"
MESSAGE = "t4"
SUBREDDIT = "t5"
AWARD = "t6"


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


def chunked(iterable: list[T] | tuple[T], n: int) -> list[list[T]]:
    d = {}
    for i, x in enumerate(iterable):
        d.setdefault(i // n, []).append(x)
    return list(d.values())


def get_project_root():
    return Path(__file__).parent.parent
