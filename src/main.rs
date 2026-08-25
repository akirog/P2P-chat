use std::collections::HashMap;
use std::sync::Arc;
use local_ip_address::local_ip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
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

        println!("broadcasting presence from {}, on port 8001", self.address);

        loop {
            broadcast_socket.send_to(
                self.username.as_bytes(),
                "255.255.255.255:8001"
            ).await?;

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }

    async fn listen_for_peers(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listen_socket = UdpSocket::bind("0.0.0.0:8001").await?;

        listen_socket.set_broadcast(false)?;

        println!("listening on {:?}", listen_socket.local_addr()?);

        let mut buffer = [0; 1024];

        loop {
            let (len, sender) = listen_socket.recv_from(&mut buffer).await?;

            let name = String::from_utf8_lossy(&buffer[..len]);
            if name == self.username { continue; }

            let mut address = sender.ip().to_string();

            address = format!("{}:8002", address);
            let mut peers = self.peers.lock().await;
            if peers.contains_key(name.as_ref()) { continue; }

            println!("received broadcast from user: {} on ip: {}", &name, address);


            let mut connection = TcpStream::connect(&address).await?;
            connection.write_all(format!("{}\r\n", self.username).as_bytes()).await?;
            peers.insert(name.to_string(), connection);

        }
    }


    async fn accept_connection_requests(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("0.0.0.0:8002").await?;
        println!("listening for tcp connections");

        loop {
            let (mut stream, addr) = listener.accept().await?;

            let mut buffer = [0; 1024];
            let length = stream.read(&mut buffer).await?;
            if length < 1 { continue; }

            let name = String::from_utf8_lossy(&buffer[..length]);
            println!("{addr} connected as {name}");

            let mut peers = self.peers.lock().await;
            if peers.contains_key(name.as_ref()) { continue; }

            peers.insert(name.to_string(), stream);
        }
    }
}



#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<String>>();

    let node = P2pNode::new(
        String::from(args.get(1).unwrap()),
        local_ip().unwrap().to_string()
    );

    println!("node name: {}", node.username);

    let node = Arc::new(node);

    let connection_node = node.clone();
    let listening_task = tokio::spawn(async move {
       if let Err(e) = connection_node.accept_connection_requests().await {
           println!("Listener: {e}");
       }
    });


    let broadcast_listener_node = node.clone();
    let broadcast_listener_task = tokio::spawn(async move {
        if let Err(e) = broadcast_listener_node.listen_for_peers().await {
            println!("Broadcast listener error: {}", e);
        }
    });


    let broadcast_node = node.clone();
    let broadcast_task = tokio::spawn(async move {
        if let Err(e) = broadcast_node.broadcast_presence().await {
            println!("Broadcasting error: {}", e);
        }
    });


    tokio::try_join!(listening_task, broadcast_listener_task, broadcast_task).unwrap();
}
