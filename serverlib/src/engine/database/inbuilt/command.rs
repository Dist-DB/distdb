use sqlparser::ast::Function;

pub trait InbuiltServerCommand {
    
    fn name(&self) -> &'static str;
    
    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String>;

}
