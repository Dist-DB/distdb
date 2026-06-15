
#[macro_export]
macro_rules! epoch {
    () => {
        $crate::helpers::utils::epoch()
    };
}


#[macro_export]
macro_rules! epoch_ms {
    () => {
        $crate::helpers::utils::epoch_ms()
    };
}


#[macro_export]
macro_rules! epoch_nanos {
    () => {
        $crate::helpers::utils::epoch_nanos()
    };
}


#[macro_export]
macro_rules! normalize_identifier {
    ($value:expr) => {
        $crate::helpers::utils::normalize_identifier($value)
    };
}


