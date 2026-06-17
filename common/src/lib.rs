
/*
	This file is part of DistDB.
	
	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.
	You should have received a copy of the GNU General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/>.

    This library provides common applcation types and utilities used across the DistDB codebase, including 
    both server and client components. It includes core types for database entities, execution plans, 
    and schema management, as well as helper functions for working with these types.

    This library is distributed under the MIT License. See the LICENSE file in the project root for more information.
	
	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

#![allow(dead_code)]

pub mod helpers;
pub mod schema;

pub use schema::{
	PeerServiceType, PeerSession, SessionLog, SessionLogEntry, SessionLogEventType, TlsMode,
};

pub const DEFAULT_SERVER_PORT: u16 = 4001;