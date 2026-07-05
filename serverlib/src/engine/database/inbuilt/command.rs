use sqlparser::ast::Function;
use std::borrow::Cow;

pub trait InbuiltServerCommand {
    
    fn name(&self) -> &'static str;

    fn usage(&self) -> Cow<'static, str> {
        Cow::Owned(format!("{}(...)", self.name()))
    }
    
    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String>;

}
