from datetime import datetime
from pathlib import Path
import tomllib

import click
import pygit2
from avulto import DME
from loguru import logger
from pygit2 import Repository
from pygit2.enums import SortMode
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ss13_codedb.git_models import (
    GitLogEntry,
    Snapshot,
    TypeDecl,
    ProcDecl,
    VarDecl,
)


@click.command()
@click.option("--settings", required=True, type=Path)
@click.option("--git_repo", required=True, type=Path)
@click.option("--branch", required=True, type=str)
def main(settings: Path, git_repo: Path, branch: str):
    logger.info("Ingesting git repo.")
    config = tomllib.load(open(settings, 'rb'))
    engine = create_engine(config["config"]["db_connection_string"])

    repo = Repository(git_repo)

    with Session(engine) as session:
        for commit in repo.walk(
            repo.branches.get(branch).raw_target, SortMode.TOPOLOGICAL
        ):
            commit_hash = str(commit.id)
            entry: GitLogEntry = (
                session.query(GitLogEntry)
                .filter(GitLogEntry.commit_hash == commit_hash)
                .scalar()
            )
            if not entry:
                results = GitLogEntry.from_commit(repo, commit)
                entry = results[0]
                session.add_all(results)

            repo.reset(commit.id, pygit2.GIT_RESET_HARD)
            dme_file = [x for x in git_repo.glob("*.dme")][0]
            logger.info(f"parsing DME @{commit.id}, {datetime.fromtimestamp(commit.commit_time)}")
            dme = DME.from_file(dme_file)
            snapshot = (
                session.query(Snapshot)
                .filter(Snapshot.git_log_entry_id == entry.id)
                .scalar()
            )
            if not snapshot:
                snapshot = Snapshot()
                snapshot.git_log_entry = entry
                session.add(snapshot)

            count = 0

            for pth in dme.typesof("/"):
                count += 1
                type_decl, created = TypeDecl.get_or_create(session, pth)
                snapshot.type_decls.append(type_decl)

                td = dme.types[pth]
                for var_name in td.var_names(declared=True, modified=True):
                    vd = td.var_decl(var_name)
                    if not vd.source_loc:
                        continue
                    nvd, created = VarDecl.get_or_create(session, vd)
                    snapshot.var_decls.append(nvd)

                for proc_name in td.proc_names(declared=True, modified=True):
                    proc_decl, created = ProcDecl.get_or_create(
                        session, pth / proc_name
                    )
                    snapshot.proc_decls.append(proc_decl)

                if count % 500 == 0:
                    logger.info(
                        f"committing after {count} paths"
                    )
                    session.commit()

            if len(session.dirty):
                logger.info(f"committing dirty entities")
                session.commit()

if __name__ == "__main__":
    main()
