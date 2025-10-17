# swen746-project
Its a project... yea...

# Installation

1. Setup .env file
  `cp example.env .env`
  then edit .env to contain correct GITHUB_TOKEN
2. Install uv python package manager
  - MAC/Linux -- Run curl in terminal: `curl -LsSf https://astral.sh/uv/install.sh | sh`
  - Windows -- Run using irm in powershell: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
  - Others -- See https://docs.astral.sh/uv/getting-started/installation/
3. Sync python environment dependency
  `uv sync`

# Methods
repo_miner.py

A command-line tool to:
    1) Fetch and normalize commits data from GitHub
    2) Fetch and normalize issues data from GitHub
    3) Summarize commits and issues that was fetched

Sub-commands:
    - fetch-commits --repo [repo_name] --out [path_to_csv] --max [optional:number_of_commits]
    - fetch-issues --repo [repo_name] --out [path_to_csv] --state [all,open,closed;default: all] --max [optional:number_of_issues]
    - summarize --issues [path_to_csv] --commits [path_to_csv]
