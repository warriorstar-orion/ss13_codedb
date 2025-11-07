# `ss13_codedb`

## Ingesting Data

1. [Install `uv`](https://docs.astral.sh/uv/getting-started/installation/).
2. Copy `settings.example.toml` to `settings.toml` and modify the connection DB string.
3. In the root directory, `uv run create_db --settings PATH_TO_SETTINGS_TOML`
4. In the root directory, `uv run generate_parsed_git --settings .\settings.example.toml --git_repo PATH_TO_PARADISE_GIT --branch master`