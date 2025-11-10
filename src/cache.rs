use std::collections::HashMap;

use dreammaker::objtree::TypeVar;
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    TryIntoModel,
};

use crate::{
    IngesterError,
    models::{self, proc_decl, type_decl, var_decl},
};

use models::proc_decl::Entity as ProcDecl;
use models::type_decl::Entity as TypeDecl;
use models::var_decl::Entity as VarDecl;

pub(crate) struct Cache {
    pub types: HashMap<String, type_decl::Model>,
    pub vars: HashMap<VarKey, var_decl::Model>,
    pub procs: HashMap<String, proc_decl::Model>,
}

type VarKey = (String, Option<String>, String);

impl Cache {
    pub(crate) fn new() -> Self {
        Cache {
            types: Default::default(),
            vars: Default::default(),
            procs: Default::default(),
        }
    }

    pub(crate) async fn get_type(
        &mut self,
        type_path: &str,
        txn: &DatabaseTransaction,
    ) -> Result<&type_decl::Model, IngesterError> {
        let path = if !type_path.starts_with("/") {
            format!("/{}", type_path)
        } else {
            type_path.to_string()
        };
        if self.types.contains_key(&path) {
            return self
                .types
                .get(&path)
                .ok_or(IngesterError::Cache("cannot get type from cache".into()));
        }
        let model = TypeDecl::find()
            .filter(type_decl::Column::Path.eq(&path))
            .one(txn)
            .await?;
        let type_decl = if let Some(type_decl) = model {
            type_decl
        } else {
            let x = type_decl::ActiveModel {
                path: Set(path.clone()),
                ..Default::default()
            }
            .save(txn)
            .await?;

            x.try_into_model()?
        };
        self.types.insert(path.clone(), type_decl.to_owned());
        self.types
            .get(&path)
            .ok_or(IngesterError::Cache("cannot get type from cache".into()))
    }

    pub(crate) async fn get_var_decl(
        &mut self,
        var_path: &str,
        var: &TypeVar,
        txn: &DatabaseTransaction,
    ) -> Result<&var_decl::Model, IngesterError> {
        let var_key = self.get_var_key(var_path.into(), var);
        if self.vars.contains_key(&var_key) {
            return self
                .vars
                .get(&var_key)
                .ok_or(IngesterError::Cache("cannot get var from cache".into()));
        }

        let mut declared_type_id = -1;
        if let Some(ref declared_type_path) = var_key.1 {
            let declared_type = self.get_type(declared_type_path.as_str(), txn).await?;
            declared_type_id = declared_type.id;
        }

        let model = VarDecl::find()
            .filter(var_decl::Column::Path.eq(var_path))
            .filter(var_decl::Column::DeclaredTypeId.eq(declared_type_id))
            .filter(var_decl::Column::JsonConstVal.eq(var_key.2.clone()))
            .one(txn)
            .await?;

        let var_decl = if let Some(var_decl) = model {
            var_decl
        } else {
            let x = var_decl::ActiveModel {
                path: Set(var_path.into()),
                declared_type_id: Set(if declared_type_id >= 0 {
                    Some(declared_type_id)
                } else {
                    None
                }),
                json_const_val: Set(var_key.2.clone()),
                ..Default::default()
            }
            .save(txn)
            .await?;

            x.try_into_model()?
        };
        self.vars.insert(var_key.clone(), var_decl.to_owned());
        self.vars
            .get(&var_key)
            .ok_or(IngesterError::Cache("cannot get var from cache".into()))
    }

    fn get_var_key(&self, var_path: String, var: &TypeVar) -> VarKey {
        let mut declared_type: Option<String> = None;
        if let Some(var_decl) = &var.declaration {
            declared_type = Some(var_decl.var_type.type_path.join("/"));
        }

        let mut json_const_val = "".to_string();
        if let Some(const_val) = &var.value.constant {
            match const_val {
                // TODO(wso): support serializing more types here
                dreammaker::constants::Constant::Null(_) => {
                    json_const_val = "null".to_string();
                }
                dreammaker::constants::Constant::Prefab(pop) => {
                    json_const_val = format!("\"{}\"", pop);
                }
                dreammaker::constants::Constant::String(ident2) => {
                    json_const_val = format!("\"{}\"", ident2);
                }
                dreammaker::constants::Constant::Resource(ident2) => {
                    json_const_val = format!("\"{}\"", ident2);
                }
                dreammaker::constants::Constant::Float(f) => {
                    json_const_val = format!("{}", f);
                }
                _ => {}
            }
        }

        (var_path.clone(), declared_type, json_const_val)
    }

    pub(crate) async fn get_proc(
        &mut self,
        path: &str,
        txn: &DatabaseTransaction,
    ) -> Result<&proc_decl::Model, IngesterError> {
        if self.procs.contains_key(path) {
            return self
                .procs
                .get(path)
                .ok_or(IngesterError::Cache("cannot get type from cache".into()));
        }
        let model = ProcDecl::find()
            .filter(proc_decl::Column::Path.eq(path))
            .one(txn)
            .await?;
        let proc_decl = if let Some(proc_decl) = model {
            proc_decl
        } else {
            let x = proc_decl::ActiveModel {
                path: Set(path.into()),
                ..Default::default()
            }
            .save(txn)
            .await?;

            x.try_into_model()?
        };
        self.procs.insert(path.into(), proc_decl.to_owned());
        self.procs
            .get(path)
            .ok_or(IngesterError::Cache("cannot get type from cache".into()))
    }
}
