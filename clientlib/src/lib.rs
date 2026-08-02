/*

	This file is part of DistDB.

	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
	See the GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>.
	
	This library provides the core client-side types and logic for DistDB, 
	including database entities, execution plans, schema management, and 
	replication. It is used by the DistDB client to interact with the DistDB server,
	send queries, and manage database connections. 

	This library is distributed under the GNU Affero General Public License v3.0. 
    See the LICENSE file in the project root for more information.

	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

use std::sync::{Arc, Mutex, Weak};

mod config;
mod error;
mod models;
mod runtime;

pub use error::ClientError;
pub use models::{
    ClientOptions, ConnectionInfo, ExecuteResponse, QueryColumnDef, QueryResponse, QueryRow,
    QueryTimings, QueryValue, TlsMode,
};

#[derive(Debug, Clone)]
pub struct DistDbClient {
    inner: Arc<Mutex<runtime::ClientInner>>,
	active_connections: Arc<Mutex<Vec<ConnectionInfo>>>,
	client_handles: Arc<Mutex<Vec<Weak<Mutex<runtime::ClientInner>>>>>,
}

#[derive(Debug, Clone)]
pub struct DistDbChannel {
	client: DistDbClient,
}

impl DistDbChannel {

	pub async fn query(&self, sql: impl Into<String>) -> Result<QueryResponse, ClientError> {
		self.client.query(sql).await
	}

	pub async fn query_as<T>(&self, sql: impl Into<String>) -> Result<Vec<T>, ClientError>
	where
		T: serde::de::DeserializeOwned,
	{
		self.client.query_as(sql).await
	}

	pub async fn execute(&self, sql: impl Into<String>) -> Result<ExecuteResponse, ClientError> {
		self.client.execute(sql).await
	}

	pub async fn set_database(&self, database: impl Into<String>) -> Result<(), ClientError> {
		self.client.set_database(database).await
	}

	pub async fn disconnect(&self) -> Result<(), ClientError> {
		self.client.disconnect().await
	}

	pub fn client(&self) -> &DistDbClient {
		&self.client
	}

}

impl DistDbClient {

	pub fn active_connections(&self) -> Result<Vec<ConnectionInfo>, ClientError> {
		let guard = self
			.active_connections
			.lock()
			.map_err(|_| ClientError::Runtime("active connection registry lock poisoned".to_string()))?;

		Ok(guard.clone())
	}

	pub async fn close_all_connections(&self) -> Result<(), ClientError> {
		runtime::close_all_connections(self).await
	}

}
