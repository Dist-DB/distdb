use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use connector::DataQuery;
use serverlib::SqlOperation;

#[derive(Debug)]
pub struct TransactionCoordinator {
    sender: Sender<CoordinatorCommand>,
}

#[derive(Debug)]
pub enum QueryRoutingDecision {
    ExecuteImmediately,
    Staged,
    Rejected(&'static str),
}

#[derive(Debug)]
enum CoordinatorCommand {

    Begin {
        session_id: String,
        reply: Sender<Result<(), &'static str>>,
    },

    BeginWithTableLocks {
        session_id: String,
        table_ids: Vec<String>,
        reply: Sender<Result<(), &'static str>>,
    },

    IsActive {
        session_id: String,
        reply: Sender<bool>,
    },

    RouteQuery {
        session_id: String,
        query: DataQuery,
        can_stage: bool,
        reply: Sender<Result<QueryRoutingDecision, &'static str>>,
    },

    Stage {
        session_id: String,
        query: DataQuery,
        reply: Sender<Result<(), &'static str>>,
    },

    GetStaged {
        session_id: String,
        reply: Sender<Result<Vec<DataQuery>, &'static str>>,
    },

    TakeForCommit {
        session_id: String,
        reply: Sender<Result<Vec<DataQuery>, &'static str>>,
    },
    
    RestoreAfterFailedCommit {
        session_id: String,
        staged: Vec<DataQuery>,
        reply: Sender<Result<(), &'static str>>,
    },

    FinalizeCommit {
        session_id: String,
        reply: Sender<Result<(), &'static str>>,
    },

    ApplyRemoteLocks {
        owner_id: String,
        table_ids: Vec<String>,
        reply: Sender<()>,
    },

    ReleaseRemoteLocks {
        owner_id: String,
        table_ids: Vec<String>,
        reply: Sender<()>,
    },
    
    Rollback {
        session_id: String,
        reply: Sender<bool>,
    },

}

impl Default for TransactionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionCoordinator {

    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<CoordinatorCommand>();
        thread::spawn(move || run_coordinator_loop(receiver));
        Self { sender }
    }

    pub fn begin(&self, session_id: &str) -> Result<(), &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::Begin {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn begin_with_table_locks(
        &self,
        session_id: &str,
        table_ids: Vec<String>,
    ) -> Result<(), &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::BeginWithTableLocks {
                session_id: session_id.to_string(),
                table_ids,
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn is_active(&self, session_id: &str) -> Result<bool, &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::IsActive {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")
    }

    pub fn route_query(
        &self,
        session_id: &str,
        query: DataQuery,
        can_stage: bool,
    ) -> Result<QueryRoutingDecision, &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::RouteQuery {
                session_id: session_id.to_string(),
                query,
                can_stage,
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn stage_query(&self, session_id: &str, query: DataQuery) -> Result<(), &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::Stage {
                session_id: session_id.to_string(),
                query,
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn staged_queries(&self, session_id: &str) -> Result<Vec<DataQuery>, &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::GetStaged {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn take_for_commit(&self, session_id: &str) -> Result<Vec<DataQuery>, &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::TakeForCommit {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn rollback(&self, session_id: &str) -> Result<bool, &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::Rollback {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")
    }

    pub fn restore_after_failed_commit(
        &self,
        session_id: &str,
        staged: Vec<DataQuery>,
    ) -> Result<(), &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::RestoreAfterFailedCommit {
                session_id: session_id.to_string(),
                staged,
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn finalize_commit(&self, session_id: &str) -> Result<(), &'static str> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send(CoordinatorCommand::FinalizeCommit {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| "transaction coordinator unavailable")?;
        reply_rx
            .recv()
            .map_err(|_| "transaction coordinator unavailable")?
    }

    pub fn apply_remote_table_locks(&self, owner_id: &str, table_ids: Vec<String>) {
        let (reply_tx, reply_rx) = mpsc::channel();

        if self
            .sender
            .send(CoordinatorCommand::ApplyRemoteLocks {
                owner_id: owner_id.to_string(),
                table_ids,
                reply: reply_tx,
            })
            .is_ok()
        {
            let _ = reply_rx.recv();
        }
    }

    pub fn release_remote_table_locks(&self, owner_id: &str, table_ids: Vec<String>) {
        let (reply_tx, reply_rx) = mpsc::channel();

        if self
            .sender
            .send(CoordinatorCommand::ReleaseRemoteLocks {
                owner_id: owner_id.to_string(),
                table_ids,
                reply: reply_tx,
            })
            .is_ok()
        {
            let _ = reply_rx.recv();
        }
    }

}

fn normalize_unique_table_ids(table_ids: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();

    for table_id in table_ids {
        let normalized_table_id = common::normalize_identifier!(table_id);

        if normalized_table_id.is_empty() {
            continue;
        }

        if seen.insert(normalized_table_id.clone()) {
            normalized.push(normalized_table_id);
        }
    }

    normalized
}

fn dml_table_ids_for_query(query: &DataQuery) -> Vec<String> {
    let Ok(statements) = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) else {
        return Vec::new();
    };

    let mut table_ids = Vec::new();

    for statement in statements {
        if !matches!(
            statement.operation,
            SqlOperation::Insert | SqlOperation::Update | SqlOperation::Delete
        ) {
            continue;
        }

        let Some(table_id) = statement.object_name else {
            continue;
        };

        table_ids.push(table_id);
    }

    normalize_unique_table_ids(table_ids)
}

fn release_session_locks(
    owner_id: &str,
    table_locks_by_table: &mut HashMap<String, String>,
    locked_tables_by_session: &mut HashMap<String, HashSet<String>>,
) {
    let Some(locked_tables) = locked_tables_by_session.remove(owner_id) else {
        return;
    };

    for table_id in locked_tables {
        if table_locks_by_table
            .get(&table_id)
            .is_some_and(|owner| owner == owner_id)
        {
            table_locks_by_table.remove(&table_id);
        }
    }
}

fn run_coordinator_loop(receiver: Receiver<CoordinatorCommand>) {

    let mut staged_by_session: HashMap<String, Vec<DataQuery>> = HashMap::new();
    let mut table_locks_by_table: HashMap<String, String> = HashMap::new();
    let mut locked_tables_by_session: HashMap<String, HashSet<String>> = HashMap::new();

    while let Ok(command) = receiver.recv() {
        
        match command {
            
            CoordinatorCommand::Begin { session_id, reply } => {
                if staged_by_session.contains_key(&session_id) {
                    let _ = reply.send(Err("transaction already active for this session"));
                    continue;
                }

                staged_by_session.insert(session_id, Vec::new());
                let _ = reply.send(Ok(()));
            },

            CoordinatorCommand::BeginWithTableLocks {
                session_id,
                table_ids,
                reply,
            } => {
                if staged_by_session.contains_key(&session_id) {
                    let _ = reply.send(Err("transaction already active for this session"));
                    continue;
                }

                let normalized_tables = normalize_unique_table_ids(table_ids);

                if normalized_tables.is_empty() {
                    let _ = reply.send(Err("lock tables requires at least one table name"));
                    continue;
                }

                let mut blocked = false;

                for table_id in &normalized_tables {
                    if let Some(owner) = table_locks_by_table.get(table_id)
                        && owner != &session_id
                    {
                        blocked = true;
                        break;
                    }
                }

                if blocked {
                    let _ = reply.send(Err("one or more requested tables are locked by another session"));
                    continue;
                }

                let lock_set = locked_tables_by_session
                    .entry(session_id.clone())
                    .or_default();

                for table_id in normalized_tables {
                    table_locks_by_table.insert(table_id.clone(), session_id.clone());
                    lock_set.insert(table_id);
                }

                staged_by_session.insert(session_id, Vec::new());
                let _ = reply.send(Ok(()));
            },

            CoordinatorCommand::IsActive { session_id, reply } => {
                let _ = reply.send(staged_by_session.contains_key(&session_id));
            },

            CoordinatorCommand::RouteQuery {
                session_id,
                query,
                can_stage,
                reply,
            } => {
                let touched_tables = dml_table_ids_for_query(&query);

                let blocked = touched_tables.iter().any(|table_id| {
                    table_locks_by_table
                        .get(table_id)
                        .is_some_and(|owner| owner != &session_id)
                });

                if blocked {
                    let _ = reply.send(Ok(QueryRoutingDecision::Rejected(
                        "table is locked by another session",
                    )));
                    continue;
                }

                if let Some(staged) = staged_by_session.get_mut(&session_id) {
                    if !can_stage {
                        let _ = reply.send(Ok(QueryRoutingDecision::Rejected(
                            "only single-statement insert/update/delete queries are allowed inside explicit transactions",
                        )));
                        continue;
                    }

                    staged.push(query);
                    let _ = reply.send(Ok(QueryRoutingDecision::Staged));
                } else {
                    let _ = reply.send(Ok(QueryRoutingDecision::ExecuteImmediately));
                }
            },

            CoordinatorCommand::Stage {
                session_id,
                query,
                reply,
            } => {
                let Some(staged) = staged_by_session.get_mut(&session_id) else {
                    let _ = reply.send(Err("no active transaction for this session"));
                    continue;
                };

                staged.push(query);
                let _ = reply.send(Ok(()));
            },

            CoordinatorCommand::GetStaged { session_id, reply } => {
                let Some(staged) = staged_by_session.get(&session_id) else {
                    let _ = reply.send(Err("no active transaction for this session"));
                    continue;
                };

                let _ = reply.send(Ok(staged.clone()));
            },

            CoordinatorCommand::TakeForCommit { session_id, reply } => {
                let Some(staged) = staged_by_session.remove(&session_id) else {
                    let _ = reply.send(Err("no active transaction for this session"));
                    continue;
                };

                let _ = reply.send(Ok(staged));
            },
            
            CoordinatorCommand::RestoreAfterFailedCommit {
                session_id,
                staged,
                reply,
            } => {
                if staged_by_session.contains_key(&session_id) {
                    let _ = reply.send(Err("transaction already active for this session"));
                    continue;
                }

                staged_by_session.insert(session_id, staged);
                let _ = reply.send(Ok(()));
            },

            CoordinatorCommand::Rollback { session_id, reply } => {
                let removed = staged_by_session.remove(&session_id).is_some();

                release_session_locks(
                    &session_id,
                    &mut table_locks_by_table,
                    &mut locked_tables_by_session,
                );

                let _ = reply.send(removed);
            },

            CoordinatorCommand::FinalizeCommit { session_id, reply } => {
                let _ = staged_by_session.remove(&session_id);

                release_session_locks(
                    &session_id,
                    &mut table_locks_by_table,
                    &mut locked_tables_by_session,
                );

                let _ = reply.send(Ok(()));
            },

            CoordinatorCommand::ApplyRemoteLocks {
                owner_id,
                table_ids,
                reply,
            } => {
                let normalized_tables = normalize_unique_table_ids(table_ids);
                let lock_set = locked_tables_by_session
                    .entry(owner_id.clone())
                    .or_default();

                for table_id in normalized_tables {
                    if table_locks_by_table
                        .get(&table_id)
                        .is_some_and(|owner| owner != &owner_id)
                    {
                        continue;
                    }

                    table_locks_by_table.insert(table_id.clone(), owner_id.clone());
                    lock_set.insert(table_id);
                }

                let _ = reply.send(());
            },

            CoordinatorCommand::ReleaseRemoteLocks {
                owner_id,
                table_ids,
                reply,
            } => {
                let normalized_tables = normalize_unique_table_ids(table_ids);

                if normalized_tables.is_empty() {
                    release_session_locks(
                        &owner_id,
                        &mut table_locks_by_table,
                        &mut locked_tables_by_session,
                    );
                    let _ = reply.send(());
                    continue;
                }

                if let Some(locked_tables) = locked_tables_by_session.get_mut(&owner_id) {
                    for table_id in normalized_tables {
                        locked_tables.remove(&table_id);
                        if table_locks_by_table
                            .get(&table_id)
                            .is_some_and(|owner| owner == &owner_id)
                        {
                            table_locks_by_table.remove(&table_id);
                        }
                    }

                    if locked_tables.is_empty() {
                        locked_tables_by_session.remove(&owner_id);
                    }
                }

                let _ = reply.send(());
            },

        }
    
    }

}
