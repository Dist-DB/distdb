
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

    The console application is distributed under the GNU General Public License. 
    See the LICENSE file in the project root for more information.
	
	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

use console::{
    bootstrap_peers_from_cli_args, connector_tls_config_from_cli_args,
    parse_console_command, ConsoleSession,
};

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = env::args().skip(1).collect::<Vec<_>>();

    let server_list = bootstrap_peers_from_cli_args(&args);
    let tls_config = connector_tls_config_from_cli_args(&args)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    if server_list.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: console <server-address>[:<port>] [servers=host1[:port],host2[:port]] [tls=off|optional|required] [tls_ca=/path/to/ca.pem]",
        )
        .into());
    }

    let mut session = ConsoleSession::new(server_list.clone(), tls_config)?;
    log::info!("console bootstrap peers: {}", server_list.join(", "));

    let mut editor = DefaultEditor::new()?;

    println!("distdb console");
    println!("type help for commands, or \\q to quit");
    println!("all commands must end with ';' to execute");

    let mut accumulated_command = String::new();

    loop {

        let prompt = if accumulated_command.trim().is_empty() {
            format!("distdb:{}> ", session.current_database.as_deref().unwrap_or("<none>"))
        } else {
            "      -> ".to_string()
        };

        match editor.readline(&prompt) {

            Ok(line) => {
                accumulated_command.push_str(&line);
                accumulated_command.push('\n');

                match parse_console_command(&accumulated_command) {

                    Ok(Some(command)) => {
                        // Add completed command to history (trimmed, without trailing newline)
                        let _ = editor.add_history_entry(accumulated_command.trim());
                        accumulated_command.clear();
                        match session.execute(command) {
                            Ok(should_continue) => {
                                if !should_continue {
                                    break;
                                }
                            }
                            Err(error) => {
                                eprintln!("error: {error}");
                            }
                        }
                    }

                    Ok(None) => {}

                    Err(error) => {
                        accumulated_command.clear();
                        println!("error: {error}");
                    }

                }
            }

            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                if !accumulated_command.trim().is_empty() {
                    accumulated_command.clear();
                    println!("aborted pending command");
                } else {
                    println!();
                    break;
                }
            }

            Err(error) => {
                eprintln!("error: {error}");
                break;
            }

        }

    }

    Ok(())
}
