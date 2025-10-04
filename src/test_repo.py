# tests/test_repo_miner.py

from datetime import datetime, timedelta

import pandas as pd
import pytest

import src.repo_miner as repo_miner

# --- Helpers for dummy GitHub API objects ---


class DummyAuthor:
    def __init__(self, name, email, date):
        self.name = name
        self.email = email
        self.date = date


class DummyCommitCommit:
    def __init__(self, author, message):
        self.author = author
        self.message = message


class DummyCommit:
    def __init__(self, sha, author, email, date, message):
        self.sha = sha
        self.commit = DummyCommitCommit(DummyAuthor(author, email, date), message)


class DummyUser:
    def __init__(self, login):
        self.login = login


class DummyIssue:
    def __init__(
        self,
        id_,
        number,
        title,
        user,
        state,
        created_at,
        closed_at,
        comments,
        is_pr=False,
    ):
        self.id = id_
        self.number = number
        self.title = title
        self.user = DummyUser(user)
        self.state = state
        self.created_at = created_at
        self.closed_at = closed_at
        self.comments = comments
        # attribute only on pull requests
        self.pull_request = DummyUser("pr") if is_pr else None


class DummyRepo:
    def __init__(
        self,
        commits,
        issues,
    ):
        self._commits = commits
        self._issues = issues

    def get_commits(self):
        return self._commits

    def get_issues(self, state="all"):
        # filter by state
        if state == "all":
            return self._issues
        return [i for i in self._issues if i.state == state]


class DummyGithub:
    _repo: DummyRepo

    def __init__(self, token):
        assert token == "fake-token"

    def get_repo(self, repo_name):
        # ignore repo_name; return repo set in test fixture
        return self._repo


@pytest.fixture(autouse=True)
def patch_env_and_github(monkeypatch):
    global gh_instance
    # Set fake token
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")
    # Patch the symbol that repo_miner uses
    monkeypatch.setattr(repo_miner, "Github", lambda *a, **kw: gh_instance)

    # Return instance so tests can use it if needed
    return gh_instance


# Helper global placeholder
gh_instance = DummyGithub("fake-token")


# --- Tests for fetch_commits ---
# An example test case
def test_fetch_commits_basic(monkeypatch):
    # Setup dummy commits
    now = datetime.now()
    commits = [
        DummyCommit("sha1", "Alice", "a@example.com", now, "Initial commit\nDetails"),
        DummyCommit("sha2", "Bob", "b@example.com", now - timedelta(days=1), "Bug fix"),
    ]
    gh_instance._repo = DummyRepo(commits, [])

    df = repo_miner.fetch_commits("octocat/Hello-World")
    assert list(df.columns) == ["sha", "author", "email", "date", "message"]
    assert len(df) == 2
    assert df.iloc[0]["message"] == "Initial commit\nDetails"


def test_fetch_commits_limit(monkeypatch):
    """Test that fetch_commits respects the max_commits limit."""
    now = datetime.now()
    commits = [
        DummyCommit("sha1", "Alice", "a@example.com", now, "Initial commit\nDetails"),
        DummyCommit("sha2", "Bob", "b@example.com", now - timedelta(days=1), "Bug fix"),
    ] * 100
    gh_instance._repo = DummyRepo(commits, [])

    df = repo_miner.fetch_commits("octocat/Hello-World", 20)
    assert df["sha"].count() == 20
    df = repo_miner.fetch_commits("octocat/Hello-World")
    assert df["sha"].count() == 200
    df = repo_miner.fetch_commits("octocat/Hello-World", None)
    assert df["sha"].count() == 200


def test_fetch_commits_empty(monkeypatch):
    """Test that fetch_commits returns empty DataFrame when no commits exist."""
    gh_instance._repo = DummyRepo([], [])

    df = repo_miner.fetch_commits("octocat/Hello-World")
    assert df["sha"].count() == 0


# --- Tests for fetch_issues ---

def test_fetch_issues_excludes_prs(monkeypatch):
    """Test that fetch_issues excludes pull requests."""
    now = datetime.now()
    issues = [
        DummyIssue(1, 1, "Bug report", "alice", "open", now, None, 0, is_pr=False),
        DummyIssue(2, 2, "Feature request", "bob", "closed", now - timedelta(days=1), now, 3, is_pr=True),
        DummyIssue(3, 3, "Another bug", "charlie", "open", now - timedelta(days=2), None, 1, is_pr=False),
    ]
    gh_instance._repo = DummyRepo([], issues)

    df = repo_miner.fetch_issues("octocat/Hello-World")
    assert list(df.columns) == ["id", "number", "title", "user", "state", "created_at", "closed_at", "comments", "open_duration_days"]
    assert len(df) == 2  # Only 2 issues, PR excluded
    assert df["number"].tolist() == [1, 3]  # PR with number 2 should be excluded


def test_fetch_issues_date_normalization(monkeypatch):
    """Test that dates are properly normalized to ISO-8601 format."""
    now = datetime.now()
    created_at = now - timedelta(days=5)
    closed_at = now - timedelta(days=1)
    
    issues = [
        DummyIssue(1, 1, "Test issue", "alice", "closed", created_at, closed_at, 0, is_pr=False),
    ]
    gh_instance._repo = DummyRepo([], issues)

    df = repo_miner.fetch_issues("octocat/Hello-World")
    assert len(df) == 1
    
    # Check that dates are ISO-8601 format
    created_iso = df.iloc[0]["created_at"]
    closed_iso = df.iloc[0]["closed_at"]
    
    # Should be ISO format strings
    assert isinstance(created_iso, str)
    assert isinstance(closed_iso, str)
    assert "T" in created_iso  # ISO format includes T
    assert "T" in closed_iso
    
    # Should be able to parse back to datetime
    parsed_created = datetime.fromisoformat(created_iso.replace('Z', '+00:00'))
    parsed_closed = datetime.fromisoformat(closed_iso.replace('Z', '+00:00'))
    assert isinstance(parsed_created, datetime)
    assert isinstance(parsed_closed, datetime)


def test_fetch_issues_open_duration_calculation(monkeypatch):
    """Test that open_duration_days is calculated correctly."""
    now = datetime.now()
    created_at = now - timedelta(days=10)
    closed_at = now - timedelta(days=3)
    
    issues = [
        DummyIssue(1, 1, "Closed issue", "alice", "closed", created_at, closed_at, 0, is_pr=False),
        DummyIssue(2, 2, "Open issue", "bob", "open", now - timedelta(days=5), None, 0, is_pr=False),
    ]
    gh_instance._repo = DummyRepo([], issues)

    df = repo_miner.fetch_issues("octocat/Hello-World")
    assert len(df) == 2
    
    # Check open_duration_days calculation
    closed_issue = df[df["state"] == "closed"].iloc[0]
    open_issue = df[df["state"] == "open"].iloc[0]
    
    # Closed issue should have 7 days duration (10 - 3 = 7)
    assert closed_issue["open_duration_days"] == 7
    
    # Open issue should have None duration
    assert pd.isna(open_issue["open_duration_days"])
