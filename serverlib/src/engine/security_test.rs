use super::{
    AccountAclEntry, AccountPrivilege, PrivilegeSelector,
};
use crate::core::identity::UserId;

#[test]
fn privilege_selector_star_maps_to_all_mysql8_privileges() {
    let selector = PrivilegeSelector::from_single_token("*")
        .expect("* should resolve as all-privileges selector");
    let set = selector.to_acl_string_set();

    assert!(set.contains("SELECT"));
    assert!(set.contains("CREATE USER"));
    assert!(set.contains("SYSTEM_USER"));
    assert!(set.len() >= 70);
}

#[test]
fn privilege_selector_null_maps_to_no_privileges() {
    let selector = PrivilegeSelector::from_single_token("NULL")
        .expect("NULL should resolve as none selector");
    let set = selector.to_acl_string_set();
    assert!(set.is_empty());
}

#[test]
fn account_acl_entry_appends_privileges_from_selector() {
    let mut acl = AccountAclEntry::new(UserId("sam".to_string()), "analytics");
    let selector = PrivilegeSelector::from_single_token("select")
        .expect("select token should resolve to explicit privilege");

    acl.append_privilege_selector(&selector);
    acl.append_privilege(AccountPrivilege::CreateUser);

    assert!(acl.acl.contains("SELECT"));
    assert!(acl.acl.contains("CREATE USER"));
}

#[test]
fn account_acl_entry_tracks_grant_option_separately() {
    let mut acl = AccountAclEntry::new(UserId("sam".to_string()), "analytics");
    let selector = PrivilegeSelector::from_single_token("*")
        .expect("star selector should resolve");

    acl.append_grant_option_for_selector(&selector);

    assert!(acl.acl.contains("SELECT"));
    assert!(acl.grant_acl.contains("SELECT"));
    assert!(acl.acl.contains("CREATE USER"));
    assert!(acl.grant_acl.contains("CREATE USER"));
}

#[test]
fn revoking_privilege_also_revokes_grant_option() {
    let mut acl = AccountAclEntry::new(UserId("sam".to_string()), "analytics");

    acl.append_grant_option_for_privilege(AccountPrivilege::Select);
    acl.append_privilege(AccountPrivilege::Select);
    acl.revoke_privilege(AccountPrivilege::Select);

    assert!(!acl.acl.contains("SELECT"));
    assert!(!acl.grant_acl.contains("SELECT"));
}

#[test]
fn usage_token_is_treated_as_no_privileges() {
    let selector = PrivilegeSelector::from_single_token("USAGE")
        .expect("USAGE should resolve as none selector");
    assert!(selector.to_acl_string_set().is_empty());
}
