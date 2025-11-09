use std::fmt::Debug;

use clap::Parser;
use dreammaker::detect_environment;
use git2::Repository;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectOptions, Database, EntityTrait, QueryFilter,
    TransactionTrait,
};

use slog::info;
use sloggers::{
    Build,
    terminal::{Destination, TerminalLoggerBuilder},
    types::Severity,
};
use thiserror::Error;

mod cache;
mod config;
mod dme;
mod models;

use models::git_log_entry;
use models::git_log_entry::Entity as GitLogEntry;

use crate::{
    cache::Cache,
    config::Config,
    dme::get_object_tree,
    models::{
        git_commit_log_numstat_entry, log_entry_from_commit, proc_decl, proc_decl_snapshot,
        snapshot, type_decl, type_decl_snapshot, var_decl, var_decl_snapshot,
    },
};

#[derive(Error, Debug)]
enum IngesterError {
    #[error("parser error")]
    Parser(String),
    #[error("cache error")]
    Cache(String),
    #[error(transparent)]
    Db(#[from] sea_orm::DbErr),
    #[error(transparent)]
    Repo(#[from] git2::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    settings: String,
    #[arg(long)]
    refpath: String,
    #[arg(long, required = false, num_args = 0, action)]
    create_tables: bool,
    #[arg(long, required = false, num_args = 0, action)]
    log_skipped_commits: bool,
}

#[tokio::main]
async fn main() -> Result<(), IngesterError> {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let mut cache = Cache::new();

    let logger = builder.build().unwrap();

    let args = Args::parse();
    let settings = std::fs::read_to_string(args.settings).expect("could not read settings file");
    let config: Config = toml::from_str(&settings).expect("could not parse settings");

    let opt = ConnectOptions::new(config.integrations.db_connection_string);
    let db = Database::connect(opt).await?;

    if args.create_tables {
        info!(logger, "creating tables");
        db.get_schema_builder()
            .register(type_decl::Entity)
            .register(var_decl::Entity)
            .register(proc_decl::Entity)
            .register(git_log_entry::Entity)
            .register(git_commit_log_numstat_entry::Entity)
            .register(proc_decl_snapshot::Entity)
            .register(snapshot::Entity)
            .register(type_decl_snapshot::Entity)
            .register(var_decl_snapshot::Entity)
            .apply(&db)
            .await?;
    }

    let repo = Repository::open(&config.environment.repo_root)?;
    let mut revwalk = repo.revwalk()?;
    revwalk.push_ref(&args.refpath)?;
    revwalk.set_sorting(git2::Sort::TIME)?;

    info!(logger, "walking revisions");

    for oid in revwalk.flatten() {
        let object = repo.find_object(oid, None)?;
        repo.reset(&object, git2::ResetType::Hard, None)?;
        let path = std::path::Path::new(&config.environment.repo_root);

        if let Ok(dme) = detect_environment(path, "paradise.dme")
            && let Some(dme_path) = dme
            && let Some(commit) = object.as_commit()
        {
            let dt = chrono::DateTime::from_timestamp(commit.time().seconds(), 0).unwrap();
            if (GitLogEntry::find()
                .filter(git_log_entry::Column::CommitHash.eq(commit.id().to_string()))
                .one(&db)
                .await?)
                .is_some()
            {
                if args.log_skipped_commits {
                    info!(
                        logger,
                        "skipping {} @{}, {}",
                        dme_path.to_string_lossy(),
                        oid,
                        dt.format("%Y-%m-%d %H:%M:%S")
                    );
                }
                continue;
            }

            let txn = db.begin().await?;
            log_entry_from_commit(&txn, &repo, commit).await?;

            info!(
                logger,
                "parsing {} @{}, {}",
                dme_path.to_string_lossy(),
                oid,
                dt.format("%Y-%m-%d %H:%M:%S")
            );
            let tree = get_object_tree(dme_path)?;

            let snapshot = snapshot::ActiveModel {
                ..Default::default()
            };
            let snapshot_insert = snapshot::Entity::insert(snapshot).exec(&txn).await?;
            let snapshot_id = snapshot_insert.last_insert_id;

            let mut count = 0;
            for type_ in tree.iter_types() {
                let td = cache.get_type(&type_.path, &txn).await?;

                let tds = type_decl_snapshot::ActiveModel {
                    snapshot_id: Set(snapshot_id),
                    type_decl_id: Set(td.id),
                };

                type_decl_snapshot::Entity::insert(tds).exec(&txn).await?;

                for (name, _) in type_.procs.iter() {
                    let proc_name = format!("{}/{}", type_.path, name);
                    let pd = cache.get_proc(&proc_name, &db, &txn).await?;
                    let pds = proc_decl_snapshot::ActiveModel {
                        snapshot_id: Set(snapshot_id),
                        proc_decl_id: Set(pd.id),
                    };

                    proc_decl_snapshot::Entity::insert(pds)
                        .on_conflict_do_nothing() // multiple defs of the same proc are fine
                        .exec(&txn)
                        .await?;
                }

                for (name, var) in type_.vars.iter() {
                    let var_path = format!("{}/{}", type_.path, name);
                    let vd = cache.get_var_decl(&var_path, var, &txn).await?;

                    let vds = var_decl_snapshot::ActiveModel {
                        snapshot_id: Set(snapshot_id),
                        var_decl_id: Set(vd.id),
                    };

                    var_decl_snapshot::Entity::insert(vds).exec(&txn).await?;
                }
                count += 1;
                if count % 1000 == 0 {
                    info!(logger, "{} paths", count)
                }
            }

            info!(logger, "committing transaction");
            txn.commit().await?;
        }
    }

    Ok(())
}
