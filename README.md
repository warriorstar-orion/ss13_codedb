# `ss13_codedb`

## Ingesting Data

1. Install rust and cargo.
2. Copy `settings.example.toml` to `settings.toml` and modify the connection DB string.
3. Ensure the specified database exists and is empty.
4. In the root directory, `cargo run -- --settings .\settings.toml --refpath
   refs/remotes/upstream/master --create-tables`. Note the full ref path must be
   specified. For local branches, use `refs/heads/branchname`.