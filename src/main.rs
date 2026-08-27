use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use futures::FutureExt;
use local_ip_address::local_ip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;


struct Peer {
    name: String,
    reader: Mutex<OwnedReadHalf>,
    writer: Mutex<OwnedWriteHalf>,
}

impl Peer {
    fn new(name: String, stream: TcpStream) -> Peer {
        let (reader, writer) = stream.into_split();

        Peer{
            name,
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
        }
    }

    async fn listen(&self) {
        loop {
            let mut message_buff = [0; 1024];

            let mut reader = self.reader.lock().await;

            let length = reader.read(&mut message_buff).await.unwrap();
            if length == 0 {
                // Connection terminated
                return;
            }

            println!("{}: {}\n", self.name, String::from_utf8_lossy(&message_buff[..length]));
        }
    }

    async fn send(&self, message: &str) {
        let mut writer = self.writer.lock().await;

        writer.write_all(message.as_bytes()).await.unwrap();
    }
}


struct P2pNode {
    username: String,
    address: String,
    peers: Arc<Mutex<HashMap<String, Arc<Peer>>>>
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

            let address = sender.ip().to_string();

            {
                let peers = self.peers.lock().await;
                if peers.contains_key(&address) { continue; }
            }


            println!("received broadcast from \"{}\" on ip: {}", &name, address);


            let mut connection = TcpStream::connect(format!("{}:8002", address)).await?;
            connection.write_all(format!("{}", self.username).as_bytes()).await?;

            println!("{address} connected as {name} | sender");

            let peer = Arc::new(Peer::new(name.to_string(), connection));

            let peers = self.peers.clone();
            let addr_clone = address.to_string();

            let listening_peer = peer.clone();
            let _listening_task = tokio::spawn(async move {
                listening_peer.listen().await;
            }.then(|_| async move {
                let mut peers = peers.lock().await;
                peers.remove(&addr_clone);
            }));


            let mut peers = self.peers.lock().await;

            peers.insert(sender.ip().to_string(), peer);

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

            {
                let peers = self.peers.lock().await;
                if peers.contains_key(&addr.to_string()) { continue; }
            }

            let name = String::from_utf8_lossy(&buffer[..length]);
            println!("{addr} connected as {name} | receiver");

            let peer = Arc::new(Peer::new(name.to_string(), stream));

            let peers = self.peers.clone();
            let addr_clone = addr.to_string();

            let listening_peer = peer.clone();
            let _listening_task = tokio::spawn(async move {
                listening_peer.listen().await;
            }.then(|_| async move {
                let mut peers = peers.lock().await;
                peers.remove(&addr_clone);
            }));

            let mut peers = self.peers.lock().await;
            peers.insert(addr.ip().to_string(), peer);
        }


    }
}



#[tokio::main]
async fn main() {

    match TcpListener::bind("0.0.0.0:8002").await {
        Ok(_) => {
            // Port is free, continue
        }
        Err(e) => {
            eprintln!("Error: Port 8002 is already in use!");
            eprintln!("Another instance might be running. Please close it and try again.");
            eprintln!("Error details: {}", e);
            std::process::exit(1);
        }
    }

    print!("Enter username: ");
    let mut name = String::new();

    std::io::stdout().flush().unwrap();

    std::io::stdin()
        .read_line(&mut name)
        .expect("failed to read from stdin");

    name = name.trim().to_string();

    let node = P2pNode::new(
        name,
        local_ip().unwrap().to_string()
    );

    println!("node name: {}", node.username);

    let node = Arc::new(node);

    let connection_node = node.clone();
    let _listening_task = tokio::spawn(async move {
       if let Err(e) = connection_node.accept_connection_requests().await {
           println!("Listener error: {e}");
       }
    });


    let broadcast_listener_node = node.clone();
    let _broadcast_listener_task = tokio::spawn(async move {
        if let Err(e) = broadcast_listener_node.listen_for_peers().await {
            println!("Broadcast listener error: {}", e);
        }
    });


    let broadcast_node = node.clone();
    let _broadcast_task = tokio::spawn(async move {
        if let Err(e) = broadcast_node.broadcast_presence().await {
            println!("Broadcasting error: {}", e);
        }
    });



    loop {

        let mut message = String::new();

        std::io::stdin()
            .read_line(&mut message)
            .expect("failed to read from stdin");

        message = message.trim().to_string();

        for peer in node.peers.lock().await.values_mut() {

            peer.send(&message).await;
        }
    }

}
