#!/usr/bin/env -S uv run --script
"""
repo_miner.py

A command-line tool to:
    1) Fetch and normalize commit data from GitHub

Sub-commands:
    - fetch-commits
"""

import argparse
import os
from functools import reduce

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from github import Auth, Commit, Github

type CommitRecords = dict[str, list]


def _get_commits_from_repo(github: Github, repo_name: str, max_commits: int | None):
    """Converts iterator to lists to prefetch all lists

    Args:
        github (Github): Github client to fetch from
        repo_name (str): name of the repository to fetch commits from
        max_commits (int | None): maximum number of commits, None is no limit and fetches all commits

    Returns:
        PaginatedList[Commit]: list of commits
    """
    if max_commits is None:
        return github.get_repo(repo_name).get_commits()
    return github.get_repo(repo_name).get_commits()[:max_commits]


def _conv_commits_to_record(commits: list[Commit.Commit]) -> CommitRecords:
    """
    Converts list of commits to records supported for dataframe.

    Args:
    - Commits: list[Commit] list of commits to convert

    Returns: CommitRecords dictionary of list as attribute values
    """

    def merge(raw_dict: CommitRecords, commit: Commit.Commit):
        raw_dict["sha"].append(commit.sha)
        raw_dict["author"].append(commit.commit.author.name)
        raw_dict["email"].append(commit.commit.author.email)
        raw_dict["date"].append(commit.commit.author.date.isoformat())
        raw_dict["message"].append(commit.commit.message)
        return raw_dict

    records: CommitRecords = {
        "sha": [],
        "author": [],
        "email": [],
        "date": [],
        "message": [],
    }
    return reduce(merge, commits, records)


def fetch_commits(repo_name: str, max_commits: int | None = None) -> pd.DataFrame:
    """
    Fetch up to `max_commits` from the specified GitHub repository.
    Returns a DataFrame with columns: sha, author, email, date, message.
    """
    # 1) Read GitHub token from environment
    ACCESS_TOKEN = os.environ.get("GITHUB_TOKEN")
    if ACCESS_TOKEN is None:
        raise KeyError(
            "GITHUB_TOKEN was not found. Make sure the environment file is properly loaded."
        )
    auth = Auth.Token(ACCESS_TOKEN)

    # 2) Initialize GitHub client and get the repo
    g = Github(auth=auth)

    # 3) Fetch commit objects (paginated by PyGitHub)
    commits: list[Commit.Commit] = list(
        _get_commits_from_repo(g, repo_name, max_commits)
    )  # pyright: ignore[reportAssignmentType]

    # 4) Normalize each commit into a record dict
    records = _conv_commits_to_record(commits)

    # 5) Build DataFrame from records
    return pd.DataFrame(records)


def main():
    """
    Parse command-line arguments and dispatch to sub-commands.
    """
    parser = argparse.ArgumentParser(
        prog="repo_miner", description="Fetch GitHub commits/issues and summarize them"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Sub-command: fetch-commits
    c1 = subparsers.add_parser("fetch-commits", help="Fetch commits and save to CSV")
    c1.add_argument("--repo", required=True, help="Repository in owner/repo format")
    c1.add_argument(
        "--max", type=int, dest="max_commits", help="Max number of commits to fetch"
    )
    c1.add_argument("--out", required=True, help="Path to output commits CSV")

    args = parser.parse_args()

    # Dispatch based on selected command
    if args.command == "fetch-commits":
        df = fetch_commits(args.repo, args.max_commits)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} commits to {args.out}")


if __name__ == "__main__":
    load_dotenv(find_dotenv())
    if os.environ.get("TEST_LOADED") is None:
        raise KeyError("Environment NOT LOADED!")
    main()
