use crate::core::identity::UserId;
use common::helpers::{aes_decrypt, aes_encrypt, stable_id};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserCredential {
    pub user_id: UserId,
    pub encrypted_password: String,
    pub password_nonce: String,
}

impl UserCredential {

    pub fn from_database_user_password(
        user_id: UserId,
        database_name: &str,
        password: &str,
        server_identifier: &str,
        first_schema_wal_timestamp_ms: Option<u64>,
    ) -> Self {

        let normalized_database_name = database_name.trim().to_ascii_lowercase();
        
        let password_nonce = build_password_nonce(
            &normalized_database_name,
            server_identifier,
            first_schema_wal_timestamp_ms,
        );

        let secret = build_password_secret(&password_nonce, server_identifier);
        let salt = salt_from_nonce(&password_nonce);

        Self {
            user_id: user_id.clone(),
            encrypted_password: aes_encrypt(password, &secret, &salt),
            password_nonce,
        }

    }

    pub fn verify_password(
        &self,
        candidate_password: &str,
        server_identifier: &str,
    ) -> bool {

        let secret = build_password_secret(&self.password_nonce, server_identifier);
        
        aes_decrypt(&self.encrypted_password, &secret) == candidate_password

    }

}

fn build_password_nonce(
    database_name: &str,
    server_identifier: &str,
    first_schema_wal_timestamp_ms: Option<u64>,
) -> String {

    let wal_seed = first_schema_wal_timestamp_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "wal-ts-unset".to_string());

    stable_id(&[
        "distdb-password-nonce",
        database_name,
        server_identifier,
        &wal_seed,
    ])

}

fn build_password_secret(password_nonce: &str, server_identifier: &str) -> String {
    
    stable_id(&[
        "distdb-password-secret",
        password_nonce,
        server_identifier,
    ])
    
}

fn salt_from_nonce(password_nonce: &str) -> [u8; 8] {
    let mut salt = [0u8; 8];
    let bytes = password_nonce.as_bytes();
    for i in 0..8 {
        salt[i] = if i < bytes.len() { bytes[i] } else { b'0' };
    }
    salt
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountPrivilege {
    Alter,
    AlterRoutine,
    Create,
    CreateRole,
    CreateRoutine,
    CreateTablespace,
    CreateTemporaryTables,
    CreateUser,
    CreateView,
    Delete,
    Drop,
    DropRole,
    Event,
    Execute,
    File,
    GrantOption,
    Index,
    Insert,
    LockTables,
    Process,
    Proxy,
    References,
    Reload,
    ReplicationClient,
    ReplicationSlave,
    Select,
    ShowDatabases,
    ShowView,
    Shutdown,
    Super,
    Trigger,
    Update,
    ApplicationPasswordAdmin,
    AuditAbortExempt,
    AuditAdmin,
    AuthenticationPolicyAdmin,
    BackupAdmin,
    BinlogAdmin,
    BinlogEncryptionAdmin,
    CloneAdmin,
    ConnectionAdmin,
    EncryptionKeyAdmin,
    FirewallAdmin,
    FirewallExempt,
    FirewallUser,
    FlushOptimizerCosts,
    FlushStatus,
    FlushTables,
    FlushUserResources,
    GroupReplicationAdmin,
    GroupReplicationStream,
    InnoDbRedoLogArchive,
    InnoDbRedoLogEnable,
    MaskingDictionariesAdmin,
    NdbStoredUser,
    PasswordlessUserAdmin,
    PersistRoVariablesAdmin,
    ReplicationApplier,
    ReplicationSlaveAdmin,
    ResourceGroupAdmin,
    ResourceGroupUser,
    RoleAdmin,
    SensitiveVariablesObserver,
    ServiceConnectionAdmin,
    SessionVariablesAdmin,
    SetUserId,
    ShowRoutine,
    SkipQueryRewrite,
    SystemUser,
    SystemVariablesAdmin,
    TableEncryptionAdmin,
    TelemetryLogAdmin,
    TpConnectionAdmin,
    VersionTokenAdmin,
    XaRecoverAdmin,
}

impl AccountPrivilege {

    pub fn as_str(self) -> &'static str {
        match self {
            AccountPrivilege::Alter => "ALTER",
            AccountPrivilege::AlterRoutine => "ALTER ROUTINE",
            AccountPrivilege::Create => "CREATE",
            AccountPrivilege::CreateRole => "CREATE ROLE",
            AccountPrivilege::CreateRoutine => "CREATE ROUTINE",
            AccountPrivilege::CreateTablespace => "CREATE TABLESPACE",
            AccountPrivilege::CreateTemporaryTables => "CREATE TEMPORARY TABLES",
            AccountPrivilege::CreateUser => "CREATE USER",
            AccountPrivilege::CreateView => "CREATE VIEW",
            AccountPrivilege::Delete => "DELETE",
            AccountPrivilege::Drop => "DROP",
            AccountPrivilege::DropRole => "DROP ROLE",
            AccountPrivilege::Event => "EVENT",
            AccountPrivilege::Execute => "EXECUTE",
            AccountPrivilege::File => "FILE",
            AccountPrivilege::GrantOption => "GRANT OPTION",
            AccountPrivilege::Index => "INDEX",
            AccountPrivilege::Insert => "INSERT",
            AccountPrivilege::LockTables => "LOCK TABLES",
            AccountPrivilege::Process => "PROCESS",
            AccountPrivilege::Proxy => "PROXY",
            AccountPrivilege::References => "REFERENCES",
            AccountPrivilege::Reload => "RELOAD",
            AccountPrivilege::ReplicationClient => "REPLICATION CLIENT",
            AccountPrivilege::ReplicationSlave => "REPLICATION SLAVE",
            AccountPrivilege::Select => "SELECT",
            AccountPrivilege::ShowDatabases => "SHOW DATABASES",
            AccountPrivilege::ShowView => "SHOW VIEW",
            AccountPrivilege::Shutdown => "SHUTDOWN",
            AccountPrivilege::Super => "SUPER",
            AccountPrivilege::Trigger => "TRIGGER",
            AccountPrivilege::Update => "UPDATE",
            AccountPrivilege::ApplicationPasswordAdmin => "APPLICATION_PASSWORD_ADMIN",
            AccountPrivilege::AuditAbortExempt => "AUDIT_ABORT_EXEMPT",
            AccountPrivilege::AuditAdmin => "AUDIT_ADMIN",
            AccountPrivilege::AuthenticationPolicyAdmin => "AUTHENTICATION_POLICY_ADMIN",
            AccountPrivilege::BackupAdmin => "BACKUP_ADMIN",
            AccountPrivilege::BinlogAdmin => "BINLOG_ADMIN",
            AccountPrivilege::BinlogEncryptionAdmin => "BINLOG_ENCRYPTION_ADMIN",
            AccountPrivilege::CloneAdmin => "CLONE_ADMIN",
            AccountPrivilege::ConnectionAdmin => "CONNECTION_ADMIN",
            AccountPrivilege::EncryptionKeyAdmin => "ENCRYPTION_KEY_ADMIN",
            AccountPrivilege::FirewallAdmin => "FIREWALL_ADMIN",
            AccountPrivilege::FirewallExempt => "FIREWALL_EXEMPT",
            AccountPrivilege::FirewallUser => "FIREWALL_USER",
            AccountPrivilege::FlushOptimizerCosts => "FLUSH_OPTIMIZER_COSTS",
            AccountPrivilege::FlushStatus => "FLUSH_STATUS",
            AccountPrivilege::FlushTables => "FLUSH_TABLES",
            AccountPrivilege::FlushUserResources => "FLUSH_USER_RESOURCES",
            AccountPrivilege::GroupReplicationAdmin => "GROUP_REPLICATION_ADMIN",
            AccountPrivilege::GroupReplicationStream => "GROUP_REPLICATION_STREAM",
            AccountPrivilege::InnoDbRedoLogArchive => "INNODB_REDO_LOG_ARCHIVE",
            AccountPrivilege::InnoDbRedoLogEnable => "INNODB_REDO_LOG_ENABLE",
            AccountPrivilege::MaskingDictionariesAdmin => "MASKING_DICTIONARIES_ADMIN",
            AccountPrivilege::NdbStoredUser => "NDB_STORED_USER",
            AccountPrivilege::PasswordlessUserAdmin => "PASSWORDLESS_USER_ADMIN",
            AccountPrivilege::PersistRoVariablesAdmin => "PERSIST_RO_VARIABLES_ADMIN",
            AccountPrivilege::ReplicationApplier => "REPLICATION_APPLIER",
            AccountPrivilege::ReplicationSlaveAdmin => "REPLICATION_SLAVE_ADMIN",
            AccountPrivilege::ResourceGroupAdmin => "RESOURCE_GROUP_ADMIN",
            AccountPrivilege::ResourceGroupUser => "RESOURCE_GROUP_USER",
            AccountPrivilege::RoleAdmin => "ROLE_ADMIN",
            AccountPrivilege::SensitiveVariablesObserver => "SENSITIVE_VARIABLES_OBSERVER",
            AccountPrivilege::ServiceConnectionAdmin => "SERVICE_CONNECTION_ADMIN",
            AccountPrivilege::SessionVariablesAdmin => "SESSION_VARIABLES_ADMIN",
            AccountPrivilege::SetUserId => "SET_USER_ID",
            AccountPrivilege::ShowRoutine => "SHOW_ROUTINE",
            AccountPrivilege::SkipQueryRewrite => "SKIP_QUERY_REWRITE",
            AccountPrivilege::SystemUser => "SYSTEM_USER",
            AccountPrivilege::SystemVariablesAdmin => "SYSTEM_VARIABLES_ADMIN",
            AccountPrivilege::TableEncryptionAdmin => "TABLE_ENCRYPTION_ADMIN",
            AccountPrivilege::TelemetryLogAdmin => "TELEMETRY_LOG_ADMIN",
            AccountPrivilege::TpConnectionAdmin => "TP_CONNECTION_ADMIN",
            AccountPrivilege::VersionTokenAdmin => "VERSION_TOKEN_ADMIN",
            AccountPrivilege::XaRecoverAdmin => "XA_RECOVER_ADMIN",
        }
    }

    pub fn all() -> &'static [AccountPrivilege] {
        const ALL: &[AccountPrivilege] = &[
            AccountPrivilege::Alter,
            AccountPrivilege::AlterRoutine,
            AccountPrivilege::Create,
            AccountPrivilege::CreateRole,
            AccountPrivilege::CreateRoutine,
            AccountPrivilege::CreateTablespace,
            AccountPrivilege::CreateTemporaryTables,
            AccountPrivilege::CreateUser,
            AccountPrivilege::CreateView,
            AccountPrivilege::Delete,
            AccountPrivilege::Drop,
            AccountPrivilege::DropRole,
            AccountPrivilege::Event,
            AccountPrivilege::Execute,
            AccountPrivilege::File,
            AccountPrivilege::GrantOption,
            AccountPrivilege::Index,
            AccountPrivilege::Insert,
            AccountPrivilege::LockTables,
            AccountPrivilege::Process,
            AccountPrivilege::Proxy,
            AccountPrivilege::References,
            AccountPrivilege::Reload,
            AccountPrivilege::ReplicationClient,
            AccountPrivilege::ReplicationSlave,
            AccountPrivilege::Select,
            AccountPrivilege::ShowDatabases,
            AccountPrivilege::ShowView,
            AccountPrivilege::Shutdown,
            AccountPrivilege::Super,
            AccountPrivilege::Trigger,
            AccountPrivilege::Update,
            AccountPrivilege::ApplicationPasswordAdmin,
            AccountPrivilege::AuditAbortExempt,
            AccountPrivilege::AuditAdmin,
            AccountPrivilege::AuthenticationPolicyAdmin,
            AccountPrivilege::BackupAdmin,
            AccountPrivilege::BinlogAdmin,
            AccountPrivilege::BinlogEncryptionAdmin,
            AccountPrivilege::CloneAdmin,
            AccountPrivilege::ConnectionAdmin,
            AccountPrivilege::EncryptionKeyAdmin,
            AccountPrivilege::FirewallAdmin,
            AccountPrivilege::FirewallExempt,
            AccountPrivilege::FirewallUser,
            AccountPrivilege::FlushOptimizerCosts,
            AccountPrivilege::FlushStatus,
            AccountPrivilege::FlushTables,
            AccountPrivilege::FlushUserResources,
            AccountPrivilege::GroupReplicationAdmin,
            AccountPrivilege::GroupReplicationStream,
            AccountPrivilege::InnoDbRedoLogArchive,
            AccountPrivilege::InnoDbRedoLogEnable,
            AccountPrivilege::MaskingDictionariesAdmin,
            AccountPrivilege::NdbStoredUser,
            AccountPrivilege::PasswordlessUserAdmin,
            AccountPrivilege::PersistRoVariablesAdmin,
            AccountPrivilege::ReplicationApplier,
            AccountPrivilege::ReplicationSlaveAdmin,
            AccountPrivilege::ResourceGroupAdmin,
            AccountPrivilege::ResourceGroupUser,
            AccountPrivilege::RoleAdmin,
            AccountPrivilege::SensitiveVariablesObserver,
            AccountPrivilege::ServiceConnectionAdmin,
            AccountPrivilege::SessionVariablesAdmin,
            AccountPrivilege::SetUserId,
            AccountPrivilege::ShowRoutine,
            AccountPrivilege::SkipQueryRewrite,
            AccountPrivilege::SystemUser,
            AccountPrivilege::SystemVariablesAdmin,
            AccountPrivilege::TableEncryptionAdmin,
            AccountPrivilege::TelemetryLogAdmin,
            AccountPrivilege::TpConnectionAdmin,
            AccountPrivilege::VersionTokenAdmin,
            AccountPrivilege::XaRecoverAdmin,
        ];

        ALL
    }

    pub fn all_string_set() -> HashSet<String> {
        Self::all()
            .iter()
            .map(|privilege| privilege.as_str().to_string())
            .collect()
    }

    pub fn from_identifier(identifier: &str) -> Option<Self> {
        let normalized = normalize_privilege_identifier(identifier)?;
        let underscored = normalized.replace(' ', "_");

        match normalized.as_str() {
            "ALTER" => Some(AccountPrivilege::Alter),
            "ALTER ROUTINE" => Some(AccountPrivilege::AlterRoutine),
            "CREATE" => Some(AccountPrivilege::Create),
            "CREATE ROLE" => Some(AccountPrivilege::CreateRole),
            "CREATE ROUTINE" => Some(AccountPrivilege::CreateRoutine),
            "CREATE TABLESPACE" => Some(AccountPrivilege::CreateTablespace),
            "CREATE TEMPORARY TABLES" => Some(AccountPrivilege::CreateTemporaryTables),
            "CREATE USER" => Some(AccountPrivilege::CreateUser),
            "CREATE VIEW" => Some(AccountPrivilege::CreateView),
            "DELETE" => Some(AccountPrivilege::Delete),
            "DROP" => Some(AccountPrivilege::Drop),
            "DROP ROLE" => Some(AccountPrivilege::DropRole),
            "EVENT" => Some(AccountPrivilege::Event),
            "EXECUTE" => Some(AccountPrivilege::Execute),
            "FILE" => Some(AccountPrivilege::File),
            "GRANT OPTION" => Some(AccountPrivilege::GrantOption),
            "INDEX" => Some(AccountPrivilege::Index),
            "INSERT" => Some(AccountPrivilege::Insert),
            "LOCK TABLES" => Some(AccountPrivilege::LockTables),
            "PROCESS" => Some(AccountPrivilege::Process),
            "PROXY" => Some(AccountPrivilege::Proxy),
            "REFERENCES" => Some(AccountPrivilege::References),
            "RELOAD" => Some(AccountPrivilege::Reload),
            "REPLICATION CLIENT" => Some(AccountPrivilege::ReplicationClient),
            "REPLICATION SLAVE" => Some(AccountPrivilege::ReplicationSlave),
            "SELECT" => Some(AccountPrivilege::Select),
            "SHOW DATABASES" => Some(AccountPrivilege::ShowDatabases),
            "SHOW VIEW" => Some(AccountPrivilege::ShowView),
            "SHUTDOWN" => Some(AccountPrivilege::Shutdown),
            "SUPER" => Some(AccountPrivilege::Super),
            "TRIGGER" => Some(AccountPrivilege::Trigger),
            "UPDATE" => Some(AccountPrivilege::Update),
            _ => match underscored.as_str() {
                "APPLICATION_PASSWORD_ADMIN" => Some(AccountPrivilege::ApplicationPasswordAdmin),
                "AUDIT_ABORT_EXEMPT" => Some(AccountPrivilege::AuditAbortExempt),
                "AUDIT_ADMIN" => Some(AccountPrivilege::AuditAdmin),
                "AUTHENTICATION_POLICY_ADMIN" => Some(AccountPrivilege::AuthenticationPolicyAdmin),
                "BACKUP_ADMIN" => Some(AccountPrivilege::BackupAdmin),
                "BINLOG_ADMIN" => Some(AccountPrivilege::BinlogAdmin),
                "BINLOG_ENCRYPTION_ADMIN" => Some(AccountPrivilege::BinlogEncryptionAdmin),
                "CLONE_ADMIN" => Some(AccountPrivilege::CloneAdmin),
                "CONNECTION_ADMIN" => Some(AccountPrivilege::ConnectionAdmin),
                "ENCRYPTION_KEY_ADMIN" => Some(AccountPrivilege::EncryptionKeyAdmin),
                "FIREWALL_ADMIN" => Some(AccountPrivilege::FirewallAdmin),
                "FIREWALL_EXEMPT" => Some(AccountPrivilege::FirewallExempt),
                "FIREWALL_USER" => Some(AccountPrivilege::FirewallUser),
                "FLUSH_OPTIMIZER_COSTS" => Some(AccountPrivilege::FlushOptimizerCosts),
                "FLUSH_STATUS" => Some(AccountPrivilege::FlushStatus),
                "FLUSH_TABLES" => Some(AccountPrivilege::FlushTables),
                "FLUSH_USER_RESOURCES" => Some(AccountPrivilege::FlushUserResources),
                "GROUP_REPLICATION_ADMIN" => Some(AccountPrivilege::GroupReplicationAdmin),
                "GROUP_REPLICATION_STREAM" => Some(AccountPrivilege::GroupReplicationStream),
                "INNODB_REDO_LOG_ARCHIVE" => Some(AccountPrivilege::InnoDbRedoLogArchive),
                "INNODB_REDO_LOG_ENABLE" => Some(AccountPrivilege::InnoDbRedoLogEnable),
                "MASKING_DICTIONARIES_ADMIN" => Some(AccountPrivilege::MaskingDictionariesAdmin),
                "NDB_STORED_USER" => Some(AccountPrivilege::NdbStoredUser),
                "PASSWORDLESS_USER_ADMIN" => Some(AccountPrivilege::PasswordlessUserAdmin),
                "PERSIST_RO_VARIABLES_ADMIN" => Some(AccountPrivilege::PersistRoVariablesAdmin),
                "REPLICATION_APPLIER" => Some(AccountPrivilege::ReplicationApplier),
                "REPLICATION_SLAVE_ADMIN" => Some(AccountPrivilege::ReplicationSlaveAdmin),
                "RESOURCE_GROUP_ADMIN" => Some(AccountPrivilege::ResourceGroupAdmin),
                "RESOURCE_GROUP_USER" => Some(AccountPrivilege::ResourceGroupUser),
                "ROLE_ADMIN" => Some(AccountPrivilege::RoleAdmin),
                "SENSITIVE_VARIABLES_OBSERVER" => Some(AccountPrivilege::SensitiveVariablesObserver),
                "SERVICE_CONNECTION_ADMIN" => Some(AccountPrivilege::ServiceConnectionAdmin),
                "SESSION_VARIABLES_ADMIN" => Some(AccountPrivilege::SessionVariablesAdmin),
                "SET_USER_ID" => Some(AccountPrivilege::SetUserId),
                "SHOW_ROUTINE" => Some(AccountPrivilege::ShowRoutine),
                "SKIP_QUERY_REWRITE" => Some(AccountPrivilege::SkipQueryRewrite),
                "SYSTEM_USER" => Some(AccountPrivilege::SystemUser),
                "SYSTEM_VARIABLES_ADMIN" => Some(AccountPrivilege::SystemVariablesAdmin),
                "TABLE_ENCRYPTION_ADMIN" => Some(AccountPrivilege::TableEncryptionAdmin),
                "TELEMETRY_LOG_ADMIN" => Some(AccountPrivilege::TelemetryLogAdmin),
                "TP_CONNECTION_ADMIN" => Some(AccountPrivilege::TpConnectionAdmin),
                "VERSION_TOKEN_ADMIN" => Some(AccountPrivilege::VersionTokenAdmin),
                "XA_RECOVER_ADMIN" => Some(AccountPrivilege::XaRecoverAdmin),
                _ => None,
            },
        }
    }

}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivilegeSelector {
    All,
    None,
    Explicit(HashSet<AccountPrivilege>),
}

impl PrivilegeSelector {

    pub fn from_single_token(token: &str) -> Option<Self> {
        
        let normalized = normalize_privilege_identifier(token)?;

        if normalized == "*" || normalized == "ALL" || normalized == "ALL PRIVILEGES" {
            return Some(PrivilegeSelector::All);
        }

        if normalized == "NULL" || normalized == "USAGE" {
            return Some(PrivilegeSelector::None);
        }

        AccountPrivilege::from_identifier(&normalized).map(|privilege| {
            PrivilegeSelector::Explicit(HashSet::from([privilege]))
        })

    }

    pub fn to_acl_string_set(&self) -> HashSet<String> {

        match self {

            PrivilegeSelector::All => AccountPrivilege::all_string_set(),

            PrivilegeSelector::None => HashSet::new(),

            PrivilegeSelector::Explicit(privileges) => privileges
                .iter()
                .map(|privilege| privilege.as_str().to_string())
                .collect(),

        }
        
    }

}

fn normalize_privilege_identifier(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed == "*" {
        return Some("*".to_string());
    }

    let normalized = trimmed
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AccountAclEntry {
    pub user_id: UserId,
    pub database_id: String,
    #[serde(default)]
    pub acl: HashSet<String>,
    #[serde(default)]
    pub grant_acl: HashSet<String>,
    #[serde(default)]
    pub object_acl: HashMap<String, HashSet<String>>,
}

impl AccountAclEntry {

    pub fn new(user_id: UserId, database_id: impl Into<String>) -> Self {
        Self {
            user_id,
            database_id: database_id.into().trim().to_ascii_lowercase(),
            acl: HashSet::new(),
            grant_acl: HashSet::new(),
            object_acl: HashMap::new(),
        }
    }

    pub fn append_privilege(&mut self, privilege: AccountPrivilege) {
        self.acl.insert(privilege.as_str().to_string());
    }

    pub fn append_privilege_selector(&mut self, selector: &PrivilegeSelector) {
        self.acl.extend(selector.to_acl_string_set());
    }

    pub fn append_grant_option_for_privilege(&mut self, privilege: AccountPrivilege) {
        self.grant_acl.insert(privilege.as_str().to_string());
    }

    pub fn append_grant_option_for_selector(&mut self, selector: &PrivilegeSelector) {
        let set = selector.to_acl_string_set();
        self.acl.extend(set.iter().cloned());
        self.grant_acl.extend(set);
    }

    pub fn revoke_privilege(&mut self, privilege: AccountPrivilege) {
        let value = privilege.as_str();
        self.acl.remove(value);
        self.grant_acl.remove(value);
    }

    pub fn revoke_grant_option_for_privilege(&mut self, privilege: AccountPrivilege) {
        self.grant_acl.remove(privilege.as_str());
    }

    pub fn append_object_privilege(
        &mut self,
        object_name: &str,
        privilege: AccountPrivilege,
    ) {

        let normalized_object = normalize_acl_object_name(object_name);
        if normalized_object.is_empty() {
            return;
        }

        self.object_acl
            .entry(normalized_object)
            .or_default()
            .insert(privilege.as_str().to_string());

    }

    pub fn revoke_object_privilege(
        &mut self,
        object_name: &str,
        privilege: AccountPrivilege,
    ) {

        let normalized_object = normalize_acl_object_name(object_name);
        if normalized_object.is_empty() {
            return;
        }

        if let Some(privileges) = self.object_acl.get_mut(&normalized_object) {
            privileges.remove(privilege.as_str());

            if privileges.is_empty() {
                self.object_acl.remove(&normalized_object);
            }
        }

    }

    pub fn has_privilege_for_object(
        &self,
        privilege: AccountPrivilege,
        object_name: Option<&str>,
    ) -> bool {

        if self.acl.contains(privilege.as_str()) {
            return true;
        }

        let Some(object_name) = object_name else {
            return false;
        };

        let normalized_object = normalize_acl_object_name(object_name);
        if normalized_object.is_empty() {
            return false;
        }

        self.object_acl
            .get(&normalized_object)
            .map(|privileges| privileges.contains(privilege.as_str()))
            .unwrap_or(false)

    }

}

fn normalize_acl_object_name(value: &str) -> String {
    value.trim().trim_matches('`').trim_matches('"').to_ascii_lowercase()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleGrant {
    pub user_id: UserId,
    pub database_id: String,
    pub role_name: String,
}

#[cfg(test)]
#[path = "security_test.rs"]
mod tests;