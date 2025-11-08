
from datetime import datetime
from pathlib import Path
import tomllib

import click
import pygit2
from avulto import DME, Path as p
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
from ss13_codedb.utils import CodeTree


@click.command()
@click.option("--settings", required=True, type=Path)
@click.option("--git_repo", required=True, type=Path)
@click.option("--branch", required=True, type=str)
def main(settings: Path, git_repo: Path, branch: str):
    # import logging
    # logging.basicConfig()
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    logger.info("Ingesting git repo.")
    config = tomllib.load(open(settings, "rb"))
    engine = create_engine(config["config"]["db_connection_string"])

    repo = Repository(git_repo)

    code = CodeTree()

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
            logger.info(
                f"parsing DME @{commit.id}, {datetime.fromtimestamp(commit.commit_time)}"
            )
            dme = DME.from_file(dme_file)
            snapshot = (
                session.query(Snapshot)
                .filter(Snapshot.git_log_entry_id == entry.id)
                .scalar()
            )
            if not snapshot:
                snapshot = Snapshot()
                snapshot.git_log_entry = entry
                # session.add(snapshot)

            count = 0

            for pth in dme.typesof("/"):
                count += 1
                # logger.debug(f"{pth}")
                if pth not in code.seen_types:
                    type_decl, _ = TypeDecl.get_or_create(session, pth)
                    code.seen_types[pth] = type_decl
                else:
                    type_decl = code.seen_types[pth]
                snapshot.type_decls.append(type_decl)

                td = dme.types[pth]
                for var_name in td.var_names(declared=True, modified=True):
                    # logger.debug(f"{pth}/var/{var_name}")
                    vd = td.var_decl(var_name)
                    if not vd.source_loc:
                        continue
                    # idc how much memory this chews up, everything about the var
                    # has to be unique for us to know we're keyed to the same row
                    key = (vd.type_path, vd.name, vd.declared_type, vd.const_val)
                    if key not in code.seen_vars:
                        nvd, created = VarDecl.get_or_create(session, vd, code)
                        code.seen_vars[key] = nvd
                    else:
                        nvd = code.seen_vars[key]
                    snapshot.var_decls.append(nvd)

                for proc_name in td.proc_names(declared=True, modified=True):
                    # logger.debug(f"{pth}/proc/{proc_name}")
                    proc_path = pth / proc_name
                    if proc_path not in code.seen_procs:
                        proc_decl, _ = ProcDecl.get_or_create(session, pth=str(proc_path))
                        code.seen_procs[proc_path] = proc_decl
                    else:
                        proc_decl = code.seen_procs[proc_path]
                    snapshot.proc_decls.append(proc_decl)

                if count % 500 == 0:
                    logger.info(f"{count} paths...")

            session.add(snapshot)
            session.commit()


if __name__ == "__main__":
    main()
