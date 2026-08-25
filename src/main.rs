use std::collections::HashMap;
use std::sync::Arc;
use local_ip_address::local_ip;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;

struct P2pNode {
    username: String,
    address: String,
    peers: Arc<Mutex<HashMap<String, TcpStream>>>
}

impl P2pNode {
    fn new(username: String, address: String) -> P2pNode {
        return P2pNode{
            username,
            address,
            peers: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    async fn broadcast_presence(&self) -> Result<(), Box<dyn std::error::Error>> {
        let broadcast_socket = UdpSocket::bind("0.0.0.0:0").await?;

        broadcast_socket.set_broadcast(true)?;

        println!("broadcasting presence from {}, on port 25", self.address);

        loop {
            broadcast_socket.send_to(
                self.username.as_bytes(),
                "255.255.255.255:25"
            ).await?;

            println!("broadcasted presence");

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }


        Ok(())
    }

    async fn listen_for_peers(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listen_socket = UdpSocket::bind("0.0.0.0:25").await?;

        listen_socket.set_broadcast(false)?;

        println!("listening on {:?}", listen_socket.local_addr()?);

        let mut buffer = [0; 1024];

        loop {
            let (len, sender) = listen_socket.recv_from(&mut buffer).await?;

            let name = String::from_utf8_lossy(&buffer[..len]);
            if name == self.username { continue; }

            println!("received message from {}", &name);

            let mut address = sender.ip().to_string();
            println!("message from ip: {}", address);

            address = format!("{}:8001", address);
            let connection = TcpStream::connect(&address).await?;

            let mut peers = self.peers.lock().await;
            peers.insert(name.to_string(), connection);

        }


        Ok(())
    }
}



#[tokio::main]
async fn main() {

    let mut node = P2pNode::new(String::from("bob"), local_ip().unwrap().to_string());

    println!("Hello, world!");
}
