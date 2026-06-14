use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use connector::DataQuery;

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

    TakeForCommit {
        session_id: String,
        reply: Sender<Result<Vec<DataQuery>, &'static str>>,
    },
    
    RestoreAfterFailedCommit {
        session_id: String,
        staged: Vec<DataQuery>,
        reply: Sender<Result<(), &'static str>>,
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

}

fn run_coordinator_loop(receiver: Receiver<CoordinatorCommand>) {

    let mut staged_by_session: HashMap<String, Vec<DataQuery>> = HashMap::new();

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

            CoordinatorCommand::IsActive { session_id, reply } => {
                let _ = reply.send(staged_by_session.contains_key(&session_id));
            },

            CoordinatorCommand::RouteQuery {
                session_id,
                query,
                can_stage,
                reply,
            } => {
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
                let _ = reply.send(removed);
            }

        }
    
    }

}
