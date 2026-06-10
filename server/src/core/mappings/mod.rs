
pub mod query;
pub mod perf;

macro_rules! dispatch_query_operation {
	($operation:expr, $on_select:expr, $on_other:expr) => {
		match $operation {
			serverlib::SqlOperation::Select => $on_select,
			_ => $on_other,
		}
	};
}

pub(crate) use dispatch_query_operation;
