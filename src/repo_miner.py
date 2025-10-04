#!/usr/bin/env -S uv run --script
"""
repo_miner.py

A command-line tool to:
    1) Fetch and normalize commit data from GitHub
    2) Fetch and normalize issue data from GitHub

Sub-commands:
    - fetch-commits
    - fetch-issues
"""

import argparse
import os
from datetime import datetime
from functools import reduce

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from github import Auth, Commit, Github, Issue

type CommitRecords = dict[str, list]
type IssueRecords = dict[str, list]


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


def _get_issues_from_repo(github: Github, repo_name: str, state: str, max_issues: int | None):
    """Converts iterator to lists to prefetch all lists

    Args:
        github (Github): Github client to fetch from
        repo_name (str): name of the repository to fetch issues from
        state (str): state of issues to fetch (all, open, closed)
        max_issues (int | None): maximum number of issues, None is no limit and fetches all issues

    Returns:
        PaginatedList[Issue]: list of issues
    """
    if max_issues is None:
        return github.get_repo(repo_name).get_issues(state=state)
    return github.get_repo(repo_name).get_issues(state=state)[:max_issues]


def _conv_issues_to_record(issues: list[Issue.Issue]) -> IssueRecords:
    """
    Converts list of issues to records supported for dataframe.

    Args:
    - Issues: list[Issue] list of issues to convert

    Returns: IssueRecords dictionary of list as attribute values
    """

    def merge(raw_dict: IssueRecords, issue: Issue.Issue):
        # Skip pull requests (they have a pull_request attribute)
        if hasattr(issue, 'pull_request') and issue.pull_request is not None:
            return raw_dict
            
        # Calculate open_duration_days
        open_duration_days = None
        if issue.closed_at is not None:
            created = issue.created_at
            closed = issue.closed_at
            if isinstance(created, str):
                created = datetime.fromisoformat(created.replace('Z', '+00:00'))
            if isinstance(closed, str):
                closed = datetime.fromisoformat(closed.replace('Z', '+00:00'))
            open_duration_days = (closed - created).days
        
        raw_dict["id"].append(issue.id)
        raw_dict["number"].append(issue.number)
        raw_dict["title"].append(issue.title)
        raw_dict["user"].append(issue.user.login)
        raw_dict["state"].append(issue.state)
        raw_dict["created_at"].append(issue.created_at.isoformat())
        raw_dict["closed_at"].append(issue.closed_at.isoformat() if issue.closed_at else None)
        raw_dict["comments"].append(issue.comments)
        raw_dict["open_duration_days"].append(open_duration_days)
        return raw_dict

    records: IssueRecords = {
        "id": [],
        "number": [],
        "title": [],
        "user": [],
        "state": [],
        "created_at": [],
        "closed_at": [],
        "comments": [],
        "open_duration_days": [],
    }
    return reduce(merge, issues, records)


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


def fetch_issues(repo_full_name: str, state: str = "all", max_issues: int = None) -> pd.DataFrame:
    """
    Fetch up to `max_issues` from the specified GitHub repository.
    Returns a DataFrame with columns: id, number, title, user, state, created_at, closed_at, comments, open_duration_days.
    Skips pull requests.
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

    # 3) Fetch issue objects (paginated by PyGitHub)
    issues: list[Issue.Issue] = list(
        _get_issues_from_repo(g, repo_full_name, state, max_issues)
    )  # pyright: ignore[reportAssignmentType]

    # 4) Normalize each issue into a record dict
    records = _conv_issues_to_record(issues)

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

    # Sub-command: fetch-issues
    c2 = subparsers.add_parser("fetch-issues", help="Fetch issues and save to CSV")
    c2.add_argument("--repo", required=True, help="Repository in owner/repo format")
    c2.add_argument(
        "--state", 
        choices=["all", "open", "closed"], 
        default="all", 
        help="State of issues to fetch (default: all)"
    )
    c2.add_argument(
        "--max", type=int, dest="max_issues", help="Max number of issues to fetch"
    )
    c2.add_argument("--out", required=True, help="Path to output issues CSV")

    args = parser.parse_args()

    # Dispatch based on selected command
    if args.command == "fetch-commits":
        df = fetch_commits(args.repo, args.max_commits)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} commits to {args.out}")
    elif args.command == "fetch-issues":
        df = fetch_issues(args.repo, args.state, args.max_issues)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} issues to {args.out}")


if __name__ == "__main__":
    load_dotenv(find_dotenv())
    print(os.environ.get("TEST_LOADED", "Environment NOT LOADED!"))
    if os.environ.get("TEST_LOADED") is None:
        raise KeyError("Environment NOT LOADED!")
    main()
