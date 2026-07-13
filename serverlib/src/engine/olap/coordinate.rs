
/// A comparable, hashable coordinate along one dimension axis.
///
/// Derived from the decoded row field value at cube construction time.
/// Floating-point values are excluded from coordinates because they are not
/// hashable; float columns should be used as measures, or bucketed into an
/// integer/text representation before being nominated as a dimension.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DimensionCoordinate {
    Null,
    Text(String),
    Integer(i64),
    Boolean(bool),
}

impl std::fmt::Display for DimensionCoordinate {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        
        match self {
            
            Self::Null                  => write!(f, "NULL"),

            Self::Text(s)      => write!(f, "{s}"),

            Self::Integer(n)      => write!(f, "{n}"),

            Self::Boolean(b)     => write!(f, "{b}"),

        }

    }

}
