
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
    parse_console_command_with_delimiter, ConsoleCommand, ConsoleSession,
};

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::io;

fn startup_user_from_cli_args(args: &[String]) -> Option<String> {
    args
        .iter()
        .find_map(|arg| arg.strip_prefix("user="))
        .map(ToOwned::to_owned)
}

fn startup_password_from_cli_args(args: &[String]) -> Option<String> {
    args
        .iter()
        .find_map(|arg| arg.strip_prefix("password="))
        .map(ToOwned::to_owned)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = env::args().skip(1).collect::<Vec<_>>();
    let startup_user = startup_user_from_cli_args(&args);
    let startup_password = startup_password_from_cli_args(&args);

    let server_list = bootstrap_peers_from_cli_args(&args);
    let tls_config = connector_tls_config_from_cli_args(&args)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    if server_list.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: console <server-address>[:<port>] [servers=host1[:port],host2[:port]] [tls=off|optional|required] [tls_ca=/path/to/ca.pem] [user=<username@peer-id>] [password=<secret>]",
        )
        .into());
    }

    let mut session = ConsoleSession::new(server_list.clone(), tls_config)?;
    log::info!("console bootstrap peers: {}", server_list.join(", "));

    if startup_password.is_some() && startup_user.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "password=<secret> requires user=<username@peer-id>",
        )
        .into());
    }

    if let Some(user_and_peer) = startup_user {

        let (user, peer_id) = user_and_peer
            .split_once('@')
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "user must be formatted as <username@peer-id>",
                )
            })?;

        if user.trim().is_empty() || peer_id.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "user must be formatted as <username@peer-id>",
            )
            .into());
        }

        let resolved_peer_id = session.startup_connect_user(user, peer_id)?;
        
        if resolved_peer_id != peer_id {
            println!(
                "notification: startup peer '{}' was not discovered; connected to discovered peer '{}'",
                peer_id, resolved_peer_id
            );
        } else {
            println!("notification: startup peer connection established");
        }

        if let Some(password) = startup_password {
            session.execute(ConsoleCommand::Sql(format!("password {}", password)))?;
            println!("notification: startup password passthrough applied");
        }

    }

    let mut editor = DefaultEditor::new()?;

    println!("Distdb console (www.distdb.com)");
    println!("Copyright (c) 2026 Sam Colak. All rights reserved.");
    println!("Type help for commands, or \\q to quit");
    println!("Default delimiter is ';' (use delimiter <token> to change)");

    let mut accumulated_command = String::new();
    let mut active_delimiter = ";".to_string();

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

                match parse_console_command_with_delimiter(
                    &accumulated_command,
                    &active_delimiter,
                ) {

                    Ok(Some(command)) => {
                        // Add completed command to history (trimmed, without trailing newline)
                        let _ = editor.add_history_entry(accumulated_command.trim());
                        accumulated_command.clear();

                        if let ConsoleCommand::SetDelimiter(next_delimiter) = &command {
                            active_delimiter = next_delimiter.clone();
                        }

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
