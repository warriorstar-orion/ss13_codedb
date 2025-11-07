import datetime
import json
from typing import List

from pygit2 import Commit, Repository
from pygit2.enums import DiffStatsFormat
from sqlalchemy import (
    Column,
    ForeignKey,
    Text,
    Table,
    Integer,
    and_,
    true,
)
from sqlalchemy.dialects.mysql import INTEGER, LONGTEXT, DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column, relationship, Session

from avulto import Path as p, Dmlist

Base = declarative_base()
metadata = Base.metadata


class GitLogEntry(Base):
    __tablename__ = "git_log_entry"

    id = Column(INTEGER(11), primary_key=True)
    commit_hash: str = Column(Text, nullable=False)
    tree_hash = Column(Text, nullable=False)
    parent_hashes = Column(Text, nullable=False)
    author_name = Column(Text, nullable=False)
    author_email = Column(Text, nullable=False)
    author_date = Column(Text, nullable=False)
    committer_name = Column(Text, nullable=False)
    committer_email = Column(Text, nullable=False)
    committer_date = Column(Text, nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(LONGTEXT(), nullable=False)

    numstat_entries: Mapped[List["GitCommitLogNumstatEntry"]] = relationship(
        back_populates="git_log_entry", innerjoin=True, lazy="joined"
    )
    snapshot: Mapped["Snapshot"] = relationship(back_populates="git_log_entry")

    @staticmethod
    def from_commit(repo: Repository, commit: Commit):
        results = []
        log_entry = GitLogEntry()

        log_entry.commit_hash = str(commit.id)
        log_entry.tree_hash = str(commit.tree.id)
        log_entry.parent_hashes = ",".join([str(x) for x in commit.parent_ids])
        log_entry.author_name = commit.author.name
        log_entry.author_email = commit.author.email
        log_entry.author_date = datetime.datetime.fromtimestamp(commit.author.time)
        log_entry.committer_name = commit.committer.name
        log_entry.committer_email = commit.committer.email
        log_entry.committer_date = datetime.datetime.fromtimestamp(
            commit.committer.time
        )
        if "\n" in commit.message:
            subject, body = commit.message.split("\n", 1)
        else:
            subject = commit.message
            body = ""
        log_entry.subject = subject
        log_entry.body = body

        results.append(log_entry)

        diff = repo.diff(commit, commit.parents[0])
        numstat = diff.stats.format(DiffStatsFormat.NUMBER, 9999)
        for numstat_line in numstat.split("\n"):
            if not numstat_line:
                continue
            add, sub, path = numstat_line.split(maxsplit=2)
            if add == "-":
                add = -1
            if sub == "-":
                sub = -1
            numstat_entry = GitCommitLogNumstatEntry()
            numstat_entry.add = add
            numstat_entry.sub = sub
            numstat_entry.path_state = path
            numstat_entry.git_log_entry = log_entry
            results.append(numstat_entry)

        return results

class GitCommitLogNumstatEntry(Base):
    __tablename__ = "git_commit_log_numstat_entry"

    id = Column(INTEGER(11), primary_key=True)
    add = Column(INTEGER(11), default=-1)
    sub = Column(INTEGER(11), default=-1)
    path_state = Column(Text, nullable=False)

    git_log_entry_id: Mapped[int] = mapped_column(ForeignKey("git_log_entry.id"))
    git_log_entry: Mapped["GitLogEntry"] = relationship(
        back_populates="numstat_entries"
    )


snapshot_type_decl_association = Table(
    "snapshot_type_decl",
    Base.metadata,
    Column("snapshot_id", Integer, ForeignKey("snapshot.id")),
    Column("type_decl_id", Integer, ForeignKey("type_decl.id")),
)

snapshot_proc_decl_association = Table(
    "snapshot_proc_decl",
    Base.metadata,
    Column("snapshot_id", Integer, ForeignKey("snapshot.id")),
    Column("proc_decl_id", Integer, ForeignKey("proc_decl.id")),
)

snapshot_var_decl_association = Table(
    "snapshot_var_decl",
    Base.metadata,
    Column("snapshot_id", Integer, ForeignKey("snapshot.id")),
    Column("var_decl_id", Integer, ForeignKey("var_decl.id")),
)


class Snapshot(Base):
    __tablename__ = "snapshot"
    id = Column(INTEGER(11), primary_key=True)
    git_log_entry_id: Mapped[int] = mapped_column(ForeignKey("git_log_entry.id"))
    git_log_entry: Mapped["GitLogEntry"] = relationship(back_populates="snapshot")

    type_decls = relationship(
        "TypeDecl", secondary=snapshot_type_decl_association, back_populates="snapshots"
    )
    proc_decls = relationship(
        "ProcDecl", secondary=snapshot_proc_decl_association, back_populates="snapshots"
    )
    var_decls = relationship(
        "VarDecl", secondary=snapshot_var_decl_association, back_populates="snapshots"
    )


class TypeDecl(Base):
    __tablename__ = "type_decl"
    id = Column(INTEGER(11), primary_key=True)
    path = Column(Text, nullable=False, index=True, unique=True)

    snapshots = relationship(
        "Snapshot",
        secondary=snapshot_type_decl_association,
        back_populates="type_decls",
    )

    @classmethod
    def get_or_create(cls, session: Session, pth: p) -> tuple["TypeDecl", bool]:
        s = str(pth)
        td = session.query(cls).filter(cls.path == s).scalar()
        if td:
            return td, False
        td = TypeDecl()
        td.path = s
        session.add(td)
        return td, True


class ProcDecl(Base):
    __tablename__ = "proc_decl"
    id = Column(INTEGER(11), primary_key=True)
    path = Column(Text, nullable=False, index=True, unique=True)

    snapshots = relationship(
        "Snapshot",
        secondary=snapshot_proc_decl_association,
        back_populates="proc_decls",
    )

    @classmethod
    def get_or_create(cls, session: Session, pth: p) -> tuple["ProcDecl", bool]:
        s = str(pth)
        td = session.query(cls).filter(cls.path == s).scalar()
        if td:
            return td, False
        td = ProcDecl()
        td.path = s
        session.add(td)
        return td, True


class VarDecl(Base):
    __tablename__ = "var_decl"
    id = Column(INTEGER(11), primary_key=True)
    path = Column(Text, nullable=False)
    declared_type_id: Mapped[int] = mapped_column(
        ForeignKey("type_decl.id"), nullable=True
    )
    declared_type: Mapped["TypeDecl"] = relationship()
    json_const_val = Column(Text, nullable=False)

    snapshots = relationship(
        "Snapshot", secondary=snapshot_var_decl_association, back_populates="var_decls"
    )

    @classmethod
    def get_or_create(cls, session: Session, vd) -> tuple["VarDecl", bool]:
        vd_path = str(vd.type_path / vd.name)
        vd_declared_type = None
        json_const_val = json.dumps(None)
        if vd.const_val:
            if isinstance(vd.const_val, p):
                json_const_val = str(vd.const_val)
            elif not isinstance(vd.const_val, Dmlist):
                json_const_val = json.dumps(vd.const_val)

        expressions = [
            cls.path == vd_path,
            cls.json_const_val == json_const_val,
        ]
        if vd.declared_type:
            vd_declared_type, created = TypeDecl.get_or_create(
                session, vd.declared_type
            )
            expressions.append(cls.declared_type_id == vd_declared_type.id)

        nvd = session.query(cls).filter(and_(true(), *expressions)).scalar()
        if nvd:
            return nvd, False
        nvd = VarDecl()
        nvd.path = vd_path
        nvd.json_const_val = json_const_val
        if vd_declared_type:
            nvd.declared_type = vd_declared_type

        session.add(nvd)
        return nvd, True
