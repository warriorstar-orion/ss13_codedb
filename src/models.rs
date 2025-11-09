use git2::DiffStatsFormat;
use sea_orm::{ActiveValue::Set, DatabaseTransaction, EntityTrait};

use crate::IngesterError;

pub mod git_log_entry {
    use chrono::Utc;
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "git_log_entry")]
    pub struct Model {
        #[sea_orm(primary_key)]
        id: i32,
        commit_hash: String,
        tree_hash: String,
        parent_hashes: String,
        author_name: String,
        author_email: String,
        author_date: chrono::DateTime<Utc>,
        committer_name: String,
        committer_email: String,
        committer_date: chrono::DateTime<Utc>,
        subject: String,
        body: String,

        #[sea_orm(has_many)]
        pub git_commit_log_numstat_entries: HasMany<super::git_commit_log_numstat_entry::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod git_commit_log_numstat_entry {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "git_commit_log_numstat_entry")]
    pub struct Model {
        #[sea_orm(primary_key)]
        id: i32,
        add: i32,
        sub: i32,
        path_state: String,
        pub git_log_entry_id: i32,
        #[sea_orm(belongs_to, from = "git_log_entry_id", to = "id")]
        git_log_entry: HasOne<super::git_log_entry::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod snapshot {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "snapshot")]
    pub struct Model {
        #[sea_orm(primary_key)]
        id: i32,
        #[sea_orm(has_many, via = "type_decl_snapshot")]
        pub type_decls: HasMany<super::type_decl::Entity>,
        #[sea_orm(has_many, via = "proc_decl_snapshot")]
        pub proc_decls: HasMany<super::proc_decl::Entity>,
        #[sea_orm(has_many, via = "var_decl_snapshot")]
        pub var_decls: HasMany<super::var_decl::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod type_decl {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "type_decl")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        path: String,
        #[sea_orm(has_many, via = "type_decl_snapshot")]
        pub snapshots: HasMany<super::snapshot::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod type_decl_snapshot {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "type_decl_snapshot")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub snapshot_id: i32,
        #[sea_orm(primary_key, auto_increment = false)]
        pub type_decl_id: i32,
        #[sea_orm(belongs_to, from = "snapshot_id", to = "id")]
        pub snapshot: Option<super::snapshot::Entity>,
        #[sea_orm(belongs_to, from = "type_decl_id", to = "id")]
        pub type_decl: Option<super::type_decl::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod proc_decl {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "proc_decl")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        path: String,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod proc_decl_snapshot {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "proc_decl_snapshot")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub snapshot_id: i32,
        #[sea_orm(primary_key, auto_increment = false)]
        pub proc_decl_id: i32,
        #[sea_orm(belongs_to, from = "snapshot_id", to = "id")]
        pub snapshot: Option<super::snapshot::Entity>,
        #[sea_orm(belongs_to, from = "proc_decl_id", to = "id")]
        pub proc_decl: Option<super::proc_decl::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod var_decl {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "var_decl")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        path: String,
        declared_type_id: Option<i32>,
        #[sea_orm(belongs_to, from = "declared_type_id", to = "id")]
        declared_type: HasOne<super::type_decl::Entity>,
        #[sea_orm(column_type = "Text")]
        json_const_val: String,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod var_decl_snapshot {
    use sea_orm::DeriveEntityModel;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "var_decl_snapshot")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub snapshot_id: i32,
        #[sea_orm(primary_key, auto_increment = false)]
        pub var_decl_id: i32,
        #[sea_orm(belongs_to, from = "snapshot_id", to = "id")]
        pub snapshot: Option<super::snapshot::Entity>,
        #[sea_orm(belongs_to, from = "var_decl_id", to = "id")]
        pub var_decl: Option<super::var_decl::Entity>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub async fn log_entry_from_commit(
    txn: &DatabaseTransaction,
    repo: &git2::Repository,
    commit: &git2::Commit<'_>,
) -> Result<(), IngesterError> {
    let msg = commit.message().unwrap();
    let (subject, body) = if msg.contains('\n') {
        msg.split_once('\n').unwrap()
    } else {
        (msg, "")
    };

    let model = git_log_entry::ActiveModel {
        commit_hash: Set(commit.id().to_string()),
        tree_hash: Set(commit.tree_id().to_string()),
        parent_hashes: Set(commit
            .parent_ids()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",")),
        author_name: Set(commit.author().name().map_or("", |f| f).to_owned()),
        author_email: Set(commit.author().email().map_or("", |f| f).to_owned()),
        author_date: Set(
            chrono::DateTime::from_timestamp_secs(commit.author().when().seconds())
                .expect("bad commit author date"),
        ),

        committer_name: Set(commit.committer().name().map_or("", |f| f).to_owned()),
        committer_email: Set(commit.committer().email().map_or("", |f| f).to_owned()),
        committer_date: Set(chrono::DateTime::from_timestamp_secs(
            commit.committer().when().seconds(),
        )
        .expect("bad commit committer date")),

        subject: Set(subject.to_owned()),
        body: Set(body.to_owned()),

        ..Default::default()
    };

    let entry = git_log_entry::Entity::insert(model).exec(txn).await?;

    let mut numstat_entries = vec![];
    let diff = repo.diff_tree_to_tree(
        Some(commit.tree().as_ref().unwrap()),
        Some(commit.parent(0).as_ref().unwrap().tree().as_ref().unwrap()),
        None,
    )?;

    let diffstats = diff.stats()?;
    let buf = diffstats.to_buf(DiffStatsFormat::NUMBER, 9999)?;
    let numstats = buf.as_str().unwrap();
    for numstat_line in numstats.split("\n") {
        let mut add = -1;
        let mut sub = -1;

        let splits: Vec<&str> = numstat_line.split_whitespace().collect();
        if splits.is_empty() {
            continue;
        }
        if splits[0] != "-" {
            add = splits[0].parse::<i32>().expect("bad diffstat add");
        }
        if splits[1] != "-" {
            sub = splits[1].parse::<i32>().expect("bad diffstat sub");
        }

        let numstat_entry = git_commit_log_numstat_entry::ActiveModel {
            git_log_entry_id: Set(entry.last_insert_id),
            add: Set(add),
            sub: Set(sub),
            path_state: Set(splits[2].to_owned()),
            ..Default::default()
        };
        numstat_entries.push(numstat_entry.to_owned());
    }

    git_commit_log_numstat_entry::Entity::insert_many(numstat_entries)
        .exec(txn)
        .await?;

    Ok(())
}
