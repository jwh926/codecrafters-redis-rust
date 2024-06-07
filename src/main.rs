use anyhow::Result;
use resp::Value;
use std::sync::{Arc, Mutex};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::net::{TcpListener, TcpStream};

mod resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(Mutex::new(
        HashMap::<String, (String, Option<Instant>)>::new(),
    ));

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        println!("accepted new connection");
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            handle_conn(stream, db).await;
        });
    }
}

async fn handle_conn(
    stream: TcpStream,
    db: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>,
) {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let value = handler.read_value().await.unwrap();
        // println!("value: {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
                // "*3\r\n$3\r\nSET\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n"
                "SET" => {
                    let key = unpack_bulk_str(args[0].clone()).unwrap();
                    let value = unpack_bulk_str(args[1].clone()).unwrap();
                    let expiry = if args.len() >= 3 {
                        let expiry_type = unpack_bulk_str(args[2].clone()).unwrap().to_lowercase();
                        let expiry_time = unpack_bulk_str(args[3].clone())
                            .unwrap()
                            .parse::<u64>()
                            .unwrap();
                        if expiry_type == "px" {
                            Some(Instant::now() + Duration::from_millis(expiry_time))
                        } else if expiry_type == "ex" {
                            Some(Instant::now() + Duration::from_secs(expiry_time))
                        } else {
                            panic!("unsupported expiry type");
                        }
                    } else {
                        None
                    };
                    // println!("expiry: {:?}", expiry);
                    let mut db = db.lock().unwrap();
                    db.insert(key, (value, expiry));
                    Value::SimpleString("OK".to_string())
                }
                "GET" => {
                    let key = unpack_bulk_str(args[0].clone()).unwrap();
                    let mut db = db.lock().unwrap();
                    match db.get(&key) {
                        Some((value, expiry)) => {
                            if let Some(expiry) = expiry {
                                if Instant::now() > *expiry {
                                    db.remove(&key);
                                    Value::Null
                                } else {
                                    Value::BulkString(value.clone())
                                }
                            } else {
                                Value::BulkString(value.clone())
                            }
                        }
                        None => Value::Null,
                    }
                }
                c => panic!("unknown command: {}", c),
            }
        } else {
            println!("closing connection");
            break;
        };

        // println!("response: {:?}", response);
        handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("unexpected format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("not a bulk string")),
    }
}
