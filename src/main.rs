mod command;
mod protocol;
mod redis;

use std::{io, sync::Arc};

use redis::Redis;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let redis: Arc<Redis> = Arc::new(Redis::new());

    loop {
        match listener.accept().await {
            Ok((s, addr)) => {
                println!("accepted new connection from {}", addr);
                let rc = Arc::clone(&redis);
                let _ = tokio::spawn(async move { rc.handler(s).await });
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
