from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Snapshot:
    repo_path: Path
    commit_hash: str
    dme_file: Path
