use std::{
    io::ErrorKind,
    mem::ManuallyDrop,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use evmap::{ReadHandleFactory, ShallowCopy, WriteHandle};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::{
    command::{Command, Expire, SetCond, parse_command},
    protocol::{ProtocolData, encode_protocol, parse_protocol},
};

#[derive(PartialEq, Eq, Hash, Clone)]
struct Entry {
    val: Arc<str>,
    expire: Option<u64>,
}

impl ShallowCopy for Entry {
    unsafe fn shallow_copy(&self) -> ManuallyDrop<Self> {
        unsafe {
            ManuallyDrop::new(Self {
                val: ManuallyDrop::into_inner(self.val.shallow_copy()),
                expire: ManuallyDrop::into_inner(self.expire.shallow_copy()),
            })
        }
    }
}

impl Entry {
    fn expires(&self, curr_ms: u64) -> bool {
        self.expire.map_or(false, |v| v < curr_ms)
        // match self.expire {
        //     None => false,
        //     Some(v) => {
        //         v < curr_ms
        //     }
        // }
    }
}

pub struct Redis {
    reader: ReadHandleFactory<Arc<str>, Entry>,
    writer: Mutex<WriteHandle<Arc<str>, Entry>>,
}

impl Redis {
    pub fn new() -> Self {
        let (reader, writer) = evmap::new();
        Self {
            reader: reader.factory(),
            writer: Mutex::new(writer),
        }
    }

    async fn handle_command(&self, cmd: Command) -> anyhow::Result<ProtocolData> {
        match cmd {
            Command::Ping => Ok(ProtocolData::SimpleString(Arc::from("PONG"))),
            Command::Echo(s) => Ok(ProtocolData::SimpleString(s)),
            Command::Get(s) => match self.reader.handle().get_one(s.as_ref()) {
                Some(v) => Ok(ProtocolData::BulkString(v.val.clone())),
                None => Ok(ProtocolData::Null),
            },
            Command::Set(opts) => {
                let reader = self.reader.handle();
                let key = opts.key.clone();
                let val = opts.val.clone();
                let mut old_ent = reader.get_one(&key).map(|g| g.as_ref().clone());
                let unix_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards");
                let unix_ms = unix_time.as_millis() as u64;

                // Expiry check
                if old_ent
                    .as_ref()
                    .map(|e| e.expires(unix_ms))
                    .unwrap_or(false)
                {
                    // Entry expires, remove entry and invalidate value.
                    let mut guard = self.writer.lock().await;
                    guard.remove(key.clone(), old_ent.unwrap()).refresh();
                    drop(guard);
                    old_ent = None;
                }

                match (opts.cond, old_ent.is_some()) {
                    // Entry exists with NX, NULL
                    (Some(SetCond::NX), true) => return Ok(ProtocolData::Null),
                    // Entry not exists with XX, NULL
                    (Some(SetCond::XX), false) => return Ok(ProtocolData::Null),
                    _ => {}
                }

                let mut entry = Entry { val, expire: None };
                if let Some(exp) = opts.expire {
                    entry.expire = match exp {
                        Expire::EX(s) => Some(unix_ms + s * 1000),
                        Expire::PX(ms) => Some(unix_ms + ms),
                        Expire::EXAT(s) => Some(s * 1000),
                        Expire::PXAT(ms) => Some(ms),
                        Expire::KEEPTTL => old_ent.as_ref().and_then(|e| e.expire),
                    };
                }
                let mut guard = self.writer.lock().await;
                if let Some(e) = old_ent.as_ref() {
                    guard.remove(key.clone(), e.clone()).refresh();
                }
                guard.insert(key, entry).refresh();
                drop(guard);

                if opts.ret_old {
                    Ok(old_ent
                        .map(|e| ProtocolData::BulkString(e.val))
                        .unwrap_or(ProtocolData::Null))
                } else {
                    Ok(ProtocolData::SimpleString(Arc::from("OK")))
                }
            } // _ => unimplemented!("Command {:?} is not implemented yet.", cmd),
        }
    }

    pub async fn handler(&self, mut stream: TcpStream) {
        let mut buf = BytesMut::with_capacity(4096);
        let mut len = 0;

        loop {
            if let Err(e) = stream.readable().await {
                eprintln!("Failed to check stream readable: {}", e);
                break;
            }

            match stream.try_read_buf(&mut buf) {
                Ok(0) => break,
                Ok(sz) => {
                    len += sz;
                    match parse_protocol(String::from_utf8_lossy(&buf[..len]).as_ref())
                        .map_err(|e| e.to_owned())
                    {
                        Ok((_, prot)) => {
                            let resp = match parse_command(prot) {
                                Ok(cmd) => match self.handle_command(cmd).await {
                                    Ok(prot) => prot,
                                    Err(e) => ProtocolData::SimpleError(Arc::from(e.to_string())),
                                },
                                Err(e) => ProtocolData::SimpleError(Arc::from(e.to_string())),
                            };
                            if let Err(e) =
                                stream.write_all(&encode_protocol(resp).as_bytes()).await
                            {
                                eprintln!("Failed to write response: {}", e);
                                break;
                            }
                            buf.clear();
                            len = 0;
                        }
                        Err(nom::Err::Incomplete(_)) => {
                            continue;
                        }
                        Err(e) => {
                            eprintln!("Malformed command: {}", e);
                            break;
                        }
                    }
                }
                Err(ref e) if (e.kind() == ErrorKind::WouldBlock) => continue,
                Err(e) => {
                    eprintln!("Failed to read command: {}", e);
                    break;
                }
            }
        }
    }
}
