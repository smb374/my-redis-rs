mod command;
mod protocol;

use std::io::{self, ErrorKind};

use anyhow::bail;
use bytes::BytesMut;
use command::{handle_command, parse_command};
use protocol::{ProtocolData, encode_protocol, parse_protocol};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

async fn handler(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut len = 0;

    loop {
        stream.readable().await?;

        match stream.try_read_buf(&mut buf) {
            Ok(0) => break,
            Ok(sz) => {
                len += sz;
                match parse_protocol(String::from_utf8_lossy(&buf[..len]).as_ref())
                    .map_err(|e| e.to_owned())
                {
                    Ok((_, prot)) => {
                        let resp = match parse_command(prot) {
                            Ok(cmd) => match handle_command(cmd) {
                                Ok(prot) => prot,
                                Err(e) => ProtocolData::SimpleError(e.to_string()),
                            },
                            Err(e) => ProtocolData::SimpleError(e.to_string()),
                        };
                        stream.write_all(&encode_protocol(resp).as_bytes()).await?;

                        buf.clear();
                        len = 0;
                    }
                    Err(nom::Err::Incomplete(_)) => {
                        continue;
                    }
                    Err(e) => {
                        bail!("Malformed command: {}", e);
                    }
                }
            }
            Err(ref e) if (e.kind() == ErrorKind::WouldBlock) => continue,
            Err(e) => bail!("Failed to read command: {}", e),
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((s, addr)) => {
                println!("accepted new connection from {}", addr);
                let _ = tokio::spawn(handler(s));
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
