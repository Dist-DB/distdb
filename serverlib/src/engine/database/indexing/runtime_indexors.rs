use super::runtime_index::{RuntimeIndexRangeBound, RuntimeIndexState};
use super::runtime_index_key_codec::RuntimeIndexNumericKind;
use super::runtime_index_key_codec::normalize_runtime_index_string_key;
use super::runtime_index_storage::RuntimeIndexStorage;
use crate::{DatabaseIndex, FieldKind};

macro_rules! define_indexor {

    ($name:ident) => {

        #[derive(Debug, Clone)]
        pub struct $name {
            state: RuntimeIndexState,
        }

        impl $name {
            pub fn new(index: DatabaseIndex) -> Self {
                let mut state = RuntimeIndexState::new();
                state.index = Some(index);
                Self { state }
            }
        }

        impl RuntimeIndexStorage for $name {
            
            fn contains(&self, key: &[Vec<u8>]) -> bool {
                self.state.contains(key)
            }

            fn insert(&mut self, key: Vec<Vec<u8>>, row_ref: Option<u64>) {
                self.state.insert_with_row_ref(key, row_ref);
            }

            fn remove(&mut self, key: &[Vec<u8>], row_ref: Option<u64>) {
                self.state.remove_with_row_ref(key, row_ref);
            }

            fn cardinality(&self) -> usize {
                self.state.cardinality()
            }

            fn row_refs_for_key(&self, key: &[Vec<u8>], limit: Option<usize>) -> Vec<u64> {
                self.state.row_refs_for_key(key, limit)
            }

            fn row_refs_for_key_range(
                &self,
                lower: Option<&RuntimeIndexRangeBound>,
                upper: Option<&RuntimeIndexRangeBound>,
                limit: Option<usize>,
            ) -> Vec<u64> {
                self.state.row_refs_for_key_range(lower, upper, limit)
            }

        }

    };

}

#[derive(Debug, Clone)]
pub struct StringIndexor {
    state: RuntimeIndexState,
    case_insensitive: bool,
}

impl StringIndexor {
    pub fn new(index: DatabaseIndex) -> Self {
        let mut state = RuntimeIndexState::new();
        state.index = Some(index);
        state.set_string_case_insensitive(true);
        Self { state, case_insensitive: true }
    }

    fn normalize_key(&self, key: &[Vec<u8>]) -> Vec<Vec<u8>> {
        key.iter()
            .map(|value| normalize_runtime_index_string_key(value, self.case_insensitive))
            .collect()
    }
}

impl RuntimeIndexStorage for StringIndexor {
    fn contains(&self, key: &[Vec<u8>]) -> bool {
        self.state.contains(&self.normalize_key(key))
    }

    fn insert(&mut self, key: Vec<Vec<u8>>, row_ref: Option<u64>) {
        self.state.insert_with_row_ref(self.normalize_key(&key), row_ref);
    }

    fn remove(&mut self, key: &[Vec<u8>], row_ref: Option<u64>) {
        self.state.remove_with_row_ref(&self.normalize_key(key), row_ref);
    }

    fn cardinality(&self) -> usize { self.state.cardinality() }

    fn row_refs_for_key(&self, key: &[Vec<u8>], limit: Option<usize>) -> Vec<u64> {
        self.state.row_refs_for_key(&self.normalize_key(key), limit)
    }

    fn row_refs_for_key_range(
        &self,
        lower: Option<&RuntimeIndexRangeBound>,
        upper: Option<&RuntimeIndexRangeBound>,
        limit: Option<usize>,
    ) -> Vec<u64> {
        let lower = lower.map(|bound| RuntimeIndexRangeBound {
            key: self.normalize_key(&bound.key), inclusive: bound.inclusive,
        });
        let upper = upper.map(|bound| RuntimeIndexRangeBound {
            key: self.normalize_key(&bound.key), inclusive: bound.inclusive,
        });
        self.state.row_refs_for_key_range(lower.as_ref(), upper.as_ref(), limit)
    }
}

define_indexor!(SignedIntegerIndexor);
define_indexor!(UnsignedIntegerIndexor);
define_indexor!(FloatIndexor);
define_indexor!(DateTimeIndexor);
define_indexor!(CompositeIndexor);

#[derive(Debug, Clone)]
pub enum DatatypeIndexor {
    String(StringIndexor),
    SignedInteger(SignedIntegerIndexor),
    UnsignedInteger(UnsignedIntegerIndexor),
    Float(FloatIndexor),
    DateTime(DateTimeIndexor),
    Composite(CompositeIndexor),
}

impl DatatypeIndexor {

    pub fn from_state(state: RuntimeIndexState) -> Self {
        Self::Composite(CompositeIndexor { state })
    }

    pub fn state(&self) -> &RuntimeIndexState {
        match self {
            Self::String(indexor) => &indexor.state,
            Self::SignedInteger(indexor) => &indexor.state,
            Self::UnsignedInteger(indexor) => &indexor.state,
            Self::Float(indexor) => &indexor.state,
            Self::DateTime(indexor) => &indexor.state,
            Self::Composite(indexor) => &indexor.state,
        }
    }

    pub fn state_mut(&mut self) -> &mut RuntimeIndexState {
        match self {
            Self::String(indexor) => &mut indexor.state,
            Self::SignedInteger(indexor) => &mut indexor.state,
            Self::UnsignedInteger(indexor) => &mut indexor.state,
            Self::Float(indexor) => &mut indexor.state,
            Self::DateTime(indexor) => &mut indexor.state,
            Self::Composite(indexor) => &mut indexor.state,
        }
    }

    pub fn for_field_kind(index: DatabaseIndex, field_kind: &FieldKind) -> Self {

        match field_kind {
            
            FieldKind::Int(_) => Self::SignedInteger(SignedIntegerIndexor::new(index)),
            
            FieldKind::UInt(_) => Self::UnsignedInteger(UnsignedIntegerIndexor::new(index)),
            
            FieldKind::Float(_) => Self::Float(FloatIndexor::new(index)),
            
            FieldKind::Date | FieldKind::DateTime | FieldKind::Timestamp => {
                Self::DateTime(DateTimeIndexor::new(index))
            },

            FieldKind::StringFixed(_) | FieldKind::Text | FieldKind::Enum(_) | FieldKind::Uuid => {
                Self::String(StringIndexor::new(index))
            },
            
            FieldKind::Spatial | FieldKind::Blob => Self::Composite(CompositeIndexor::new(index)),

        }

    }

}

macro_rules! delegate_indexor {

    ($self:expr, $method:ident $(, $arg:expr)*) => {

        match $self {
            
            DatatypeIndexor::String(indexor) => indexor.$method($($arg),*),
            
            DatatypeIndexor::SignedInteger(indexor) => indexor.$method($($arg),*),
            
            DatatypeIndexor::UnsignedInteger(indexor) => indexor.$method($($arg),*),
            
            DatatypeIndexor::Float(indexor) => indexor.$method($($arg),*),
            
            DatatypeIndexor::DateTime(indexor) => indexor.$method($($arg),*),
            
            DatatypeIndexor::Composite(indexor) => indexor.$method($($arg),*),

        }
        
    };

}

impl RuntimeIndexStorage for DatatypeIndexor {

    fn contains(&self, key: &[Vec<u8>]) -> bool { delegate_indexor!(self, contains, key) }
    fn insert(&mut self, key: Vec<Vec<u8>>, row_ref: Option<u64>) { delegate_indexor!(self, insert, key, row_ref) }
    fn remove(&mut self, key: &[Vec<u8>], row_ref: Option<u64>) { delegate_indexor!(self, remove, key, row_ref) }
    fn cardinality(&self) -> usize { delegate_indexor!(self, cardinality) }
    
    fn row_refs_for_key(&self, key: &[Vec<u8>], limit: Option<usize>) -> Vec<u64> {
        delegate_indexor!(self, row_refs_for_key, key, limit)
    }
    
    fn row_refs_for_key_range(
        &self,
        lower: Option<&RuntimeIndexRangeBound>,
        upper: Option<&RuntimeIndexRangeBound>,
        limit: Option<usize>,
    ) -> Vec<u64> {
        delegate_indexor!(self, row_refs_for_key_range, lower, upper, limit)
    }

}

pub fn numeric_kind_for_field_kind(field_kind: &FieldKind) -> Option<RuntimeIndexNumericKind> {

    match field_kind {
        FieldKind::Int(_) => Some(RuntimeIndexNumericKind::Signed),
        FieldKind::UInt(_) => Some(RuntimeIndexNumericKind::Unsigned),
        _ => None,
    }

}
