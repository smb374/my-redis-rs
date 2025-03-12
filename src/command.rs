use std::sync::Arc;

use thiserror::Error;

use crate::protocol::ProtocolData;

#[derive(Debug)]
pub enum SetCond {
    NX,
    XX,
}

#[derive(Debug)]
pub enum Expire {
    EX(u64),
    PX(u64),
    EXAT(u64),
    PXAT(u64),
    KEEPTTL,
}

#[derive(Debug, Default)]
pub struct SetOpts {
    pub key: Arc<str>,
    pub val: Arc<str>,
    pub cond: Option<SetCond>,
    pub ret_old: bool,
    pub expire: Option<Expire>,
}

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(Arc<str>),
    Set(SetOpts),
    Get(Arc<str>),
}

#[derive(Debug, Error)]
pub enum ParseCommandError {
    #[error("Unrecognized command '{0}'")]
    UnrecognizedCommand(Arc<str>),
    #[error("Wrong data type")]
    WrongProtocolDataType,
    #[error("Wrong argument type or number")]
    WrongArguments,
}

fn parse_command_like(prot: &ProtocolData) -> Result<(Arc<str>, Vec<Arc<str>>), ParseCommandError> {
    match prot {
        ProtocolData::Array(v) => match v[0] {
            ProtocolData::BulkString(ref s) => {
                let cmd = Arc::from(s.to_uppercase());
                let mut args = Vec::with_capacity(v.len() - 1);

                for i in 1..v.len() {
                    if let ProtocolData::BulkString(ref arg) = v[i] {
                        args.push(arg.clone());
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
    let (cmd, args) = parse_command_like(&prot)?;
    match cmd.as_ref() {
        "PING" => {
            if args.is_empty() {
                Ok(Command::Ping)
            } else {
                Err(ParseCommandError::WrongArguments)
            }
        }
        "ECHO" => match args.len() {
            1 => Ok(Command::Echo(args[0].to_owned())),
            _ => Err(ParseCommandError::WrongArguments),
        },
        "GET" => match args.len() {
            1 => Ok(Command::Get(args[0].to_owned())),
            _ => Err(ParseCommandError::WrongArguments),
        },
        "SET" => match args.len() {
            len if len >= 2 => {
                let mut opts = SetOpts::default();
                opts.key = args[0].clone();
                opts.val = args[1].clone();

                let mut idx = 2;
                while idx < args.len() {
                    match args[idx].to_uppercase().as_str() {
                        "GET" => {
                            opts.ret_old = true;
                            idx += 1;
                        }
                        "XX" | "NX" if opts.cond.is_none() => {
                            match args[idx].as_ref() {
                                "XX" => opts.cond = Some(SetCond::XX),
                                "NX" => opts.cond = Some(SetCond::NX),
                                _ => unreachable!(),
                            }
                            idx += 1;
                        }
                        "KEEPTTL" if opts.expire.is_none() => {
                            opts.expire = Some(Expire::KEEPTTL);
                            idx += 1;
                        }
                        "EX" | "PX" | "EXAT" | "PXAT"
                            if opts.expire.is_none() && idx + 1 < args.len() =>
                        {
                            let tval = u64::from_str_radix(args[idx + 1].as_ref(), 10)
                                .map_err(|_| ParseCommandError::WrongArguments)?;
                            match args[idx].as_ref() {
                                "EX" => opts.expire = Some(Expire::EX(tval)),
                                "PX" => opts.expire = Some(Expire::PX(tval)),
                                "EXAT" => opts.expire = Some(Expire::EXAT(tval)),
                                "PXAT" => opts.expire = Some(Expire::PXAT(tval)),
                                _ => unreachable!(),
                            }
                            idx += 2;
                        }
                        _ => return Err(ParseCommandError::WrongArguments),
                    }
                }
                Ok(Command::Set(opts))
            }
            _ => Err(ParseCommandError::WrongArguments),
        },
        _ => Err(ParseCommandError::UnrecognizedCommand(cmd.to_owned())),
    }
}
