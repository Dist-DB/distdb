use console::{parse_console_command, ConsoleSession};

use std::env;
use std::io::{self, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let server_address = env::args().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: console <server-address>[:<port>]",
        )
    })?;

    let mut session = ConsoleSession::new(server_address)?;

    println!("distdb console");
    println!("type help for commands, or \\q to quit");
    println!("all commands must end with ';' to execute");

    let mut accumulated_command = String::new();

    loop {

        print!(
            "distdb:{}> ",
            session.current_database.as_deref().unwrap_or("<none>")
        );
        
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes_read = io::stdin().read_line(&mut line)?;

        if bytes_read == 0 {
            
            if !accumulated_command.trim().is_empty() {
                accumulated_command.clear();
                println!("aborted pending command");
                continue;
            }
            
            println!();
            break;
        }

        accumulated_command.push_str(&line);

        match parse_console_command(&accumulated_command) {
            
            Ok(Some(command)) => {
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

    Ok(())

}
