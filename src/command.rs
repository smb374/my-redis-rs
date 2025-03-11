use anyhow::bail;
use thiserror::Error;

use crate::protocol::{self, ProtocolData};

pub enum Command {
    Ping,
    Echo(String),
}

#[derive(Debug, Error)]
pub enum ParseCommandError {
    #[error("Unrecognized command '{0}'")]
    UnrecognizedCommand(String),
    #[error("Wrong data type")]
    WrongProtocolDataType,
    #[error("Wrong argument count")]
    WrongArgumentCount,
}

fn parse_command_like(prot: ProtocolData) -> Result<(String, Vec<String>), ParseCommandError> {
    match prot {
        ProtocolData::Array(v) => match v[0] {
            ProtocolData::BulkString(ref s) => {
                let cmd = s.to_owned();
                let mut args = Vec::with_capacity(v.len() - 1);

                for i in 1..v.len() {
                    if let ProtocolData::BulkString(ref arg) = v[i] {
                        args.push(arg.to_owned());
                    } else {
                        return Err(ParseCommandError::WrongProtocolDataType);
                    }
                }

                Ok((cmd, args))
            }
            _ => Err(ParseCommandError::WrongProtocolDataType),
        },
        _ => Err(ParseCommandError::WrongProtocolDataType),
    }
}

pub fn parse_command(prot: ProtocolData) -> Result<Command, ParseCommandError> {
    let (cmd, args) = parse_command_like(prot)?;
    match cmd.as_str() {
        "PING" => {
            if args.is_empty() {
                Ok(Command::Ping)
            } else {
                Err(ParseCommandError::WrongArgumentCount)
            }
        }
        "ECHO" => match args.len() {
            1 => Ok(Command::Echo(args[0].to_owned())),
            _ => Err(ParseCommandError::WrongArgumentCount),
        },
        _ => Err(ParseCommandError::UnrecognizedCommand(cmd)),
    }
}

pub fn handle_command(cmd: Command) -> anyhow::Result<ProtocolData> {
    match cmd {
        Command::Ping => Ok(ProtocolData::SimpleString("PONG".to_string())),
        Command::Echo(s) => Ok(ProtocolData::SimpleString(s)),
    }
}
