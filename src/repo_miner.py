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


def _get_issues_from_repo(
    github: Github, repo_name: str, state: str, max_issues: int | None
):
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

    def merge(raw_dict: IssueRecords, issue: Issue.Issue) -> IssueRecords:
        # Skip pull requests (they have a pull_request attribute)
        if hasattr(issue, "pull_request") and issue.pull_request is not None:
            return raw_dict

        # Calculate open_duration_days
        open_duration_days = None
        if issue.closed_at is not None:
            created = issue.created_at
            closed = issue.closed_at
            open_duration_days = (closed - created).days

        raw_dict["id"].append(issue.id)
        raw_dict["number"].append(issue.number)
        raw_dict["title"].append(issue.title)
        raw_dict["user"].append(issue.user.login)
        raw_dict["state"].append(issue.state)
        raw_dict["created_at"].append(issue.created_at.isoformat())
        raw_dict["closed_at"].append(
            issue.closed_at.isoformat() if issue.closed_at else None
        )
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


def fetch_issues(
    repo_full_name: str, state: str = "all", max_issues: int | None = None
) -> pd.DataFrame:
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


def csv_to_df(path: str):
    """Takes a path to csv and converts to pd.df"""
    if not isinstance(path, str) or path.strip() == "":
        raise ValueError("`path` must be a non-empty string pointing to a CSV file")
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    return pd.read_csv(path)


def merge_and_summarize(commits_df, issues_df):
    """
    Takes two DataFrames (commits and issues) and prints:
        - Top 5 committers by commit count
        - Issue close rate (closed/total)
        - Average open duration for closed issues (in days)
    """
    # Copy to avoid modifying original data
    commits = commits_df.copy()
    issues = issues_df.copy()

    # 1) Normalize date/time columns to pandas datetime
    commits["date"] = pd.to_datetime(commits["date"], errors="coerce")
    issues["created_at"] = pd.to_datetime(issues["created_at"], errors="coerce")
    issues["closed_at"] = pd.to_datetime(issues["closed_at"], errors="coerce")

    # 2) Top 5 committers
    top_committers = (
        commits["author"].fillna("<unknown>").value_counts().head(5).to_dict()
    )

    # 3) Calculate issue close rate
    total_issues = len(issues)
    closed_issues = int((issues["state"].fillna("") == "closed").sum())
    close_rate = None
    if total_issues > 0:
        close_rate = closed_issues / total_issues

    # 4) Compute average open duration (days) for closed issues
    avg_open_days = None
    if "open_duration_days" in issues.columns:
        odays = pd.to_numeric(issues["open_duration_days"], errors="coerce")
        avg_open_days = (
            float(odays.dropna().mean()) if not odays.dropna().empty else None
        )
    else:
        closed_mask = issues["closed_at"].notna() & issues["created_at"].notna()
        if closed_mask.any():
            durations = (
                issues.loc[closed_mask, "closed_at"]
                - issues.loc[closed_mask, "created_at"]
            ).dt.days
            avg_open_days = float(durations.mean())

    # Print summary to stdout
    print("Top 5 committers:")
    for author, count in top_committers.items():
        print(f"{author}: {count}")

    if close_rate is None:
        print("No issues to compute close rate.")
    else:
        print(f"Issue close rate: {closed_issues}/{total_issues} = {close_rate:.2%}")

    if avg_open_days is None:
        print("No closed issues to compute average open duration.")
    else:
        print(f"Average open duration (days) for closed issues: {avg_open_days:.2f}")

    return {
        "top_committers": top_committers,
        "total_issues": total_issues,
        "closed_issues": closed_issues,
        "close_rate": close_rate,
        "avg_open_days": avg_open_days,
    }


def main() -> None:
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
        help="State of issues to fetch (default: all)",
    )
    c2.add_argument(
        "--max", type=int, dest="max_issues", help="Max number of issues to fetch"
    )
    c2.add_argument("--out", required=True, help="Path to output issues CSV")

    # Sub-command: summarize
    c3 = subparsers.add_parser("summarize", help="Summarizes github repo")
    c3.add_argument("--commits", required=True, help="Path to commit list file(csv)")
    c3.add_argument("--issues", required=True, help="Path to issues list file(csv)")

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
    elif args.command == "summarize":
        commits_df, issues_df = csv_to_df(args.commits), csv_to_df(args.issues)
        merge_and_summarize(commits_df, issues_df)


if __name__ == "__main__":
    load_dotenv(find_dotenv())
    print(os.environ.get("TEST_LOADED", "Environment NOT LOADED!"))
    if os.environ.get("TEST_LOADED") is None:
        raise KeyError("Environment NOT LOADED!")
    main()
