use super::*;

#[test]
fn client_error_display_formats_all_variants() {
    assert_eq!(ClientError::Config("bad".to_string()).to_string(), "config error: bad");
    assert_eq!(
        ClientError::Transport("down".to_string()).to_string(),
        "transport error: down"
    );
    assert_eq!(
        ClientError::Protocol("oops".to_string()).to_string(),
        "protocol error: oops"
    );
    assert_eq!(ClientError::Decode("x".to_string()).to_string(), "decode error: x");
    assert_eq!(
        ClientError::Runtime("panic".to_string()).to_string(),
        "runtime error: panic"
    );
}

#[test]
fn from_wire_error_maps_expected_variants() {
    assert_eq!(
        ClientError::from(WireError::Transport("offline".to_string())),
        ClientError::Transport("offline".to_string())
    );

    assert_eq!(
        ClientError::from(WireError::Rejected("denied".to_string())),
        ClientError::Protocol("denied".to_string())
    );

    assert_eq!(
        ClientError::from(WireError::InvalidResponse("bad payload".to_string())),
        ClientError::Protocol("bad payload".to_string())
    );
}
