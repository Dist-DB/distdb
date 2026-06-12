use console::{parse_console_command, ConsoleSession};

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let server_address = env::args().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: console <server-address>[:<port>]",
        )
    })?;

    let mut session = ConsoleSession::new(server_address)?;
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
