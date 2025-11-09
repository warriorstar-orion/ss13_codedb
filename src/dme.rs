use std::path::PathBuf;

use dreammaker::objtree::ObjectTree;

use crate::IngesterError;

pub(crate) fn get_object_tree(path: PathBuf) -> Result<ObjectTree, IngesterError> {
    if !path.is_file() {
        return Err(IngesterError::Parser(format!("file not found: {:?}", path)));
    }
    let ctx = dreammaker::Context::default();
    let pp = match dreammaker::preprocessor::Preprocessor::new(&ctx, path.clone()) {
        Ok(pp) => pp,
        Err(e) => {
            return Err(IngesterError::Parser(format!(
                "error opening {:?}: {}",
                path, e
            )));
        }
    };
    let indents = dreammaker::indents::IndentProcessor::new(&ctx, pp);
    let mut parser = dreammaker::parser::Parser::new(&ctx, indents);

    parser.enable_procs();

    let (fatal_errored, tree) = parser.parse_object_tree_2();
    if fatal_errored {
        Err(IngesterError::Parser(format!(
            "failed to parse DME environment {:?}",
            path
        )))
    } else {
        Ok(tree)
    }
}