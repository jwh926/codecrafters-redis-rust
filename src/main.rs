use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn handle_client(mut stream: std::net::TcpStream) {
    println!("Accepted new connection");
    loop {
        let mut buf: [u8; 1024] = [0; 1024];
        match stream.read(&mut buf) {
            Ok(0) => {
                // Connection was closed by the client
                println!("Client closed the connection");
                break;
            }
            Ok(_) => {
                // Echo back a PONG response
                if let Err(e) = stream.write(b"+PONG\r\n") {
                    println!("Failed to write to stream: {}", e);
                    break;
                }
            }
            Err(e) => {
                println!("Error reading from stream: {}", e);
                break;
            }
        }
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                // Spawn a new thread to handle the connection
                thread::spawn(move || handle_client(s));
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}
