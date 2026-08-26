use std::collections::HashMap;
use std::sync::Arc;
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
            println!("Received: {}", String::from_utf8_lossy(&message_buff[..length]));
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
    peers: Arc<Mutex<HashMap<String, Peer>>>
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
                let mut peers = self.peers.lock().await;
                if peers.contains_key(&address) { continue; }
            }
            

            println!("received broadcast from \"{}\" on ip: {}", &name, address);


            let mut connection = TcpStream::connect(format!("{}:8002", address)).await?;
            connection.write_all(format!("{}", self.username).as_bytes()).await?;

            println!("{address} connected as {name} | sender");

            let peer = Peer::new(name.to_string(), connection);

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
    
            let mut peers = self.peers.lock().await;
            if peers.contains_key(&addr.to_string()) { continue; }

            let name = String::from_utf8_lossy(&buffer[..length]);
            println!("{addr} connected as {name} | receiver");

            let peer = Peer::new(name.to_string(), stream);

            let listening_peer = Arc::new(peer);
            let listening_task = tokio::spawn(async move {
                listening_peer.listen().await;
            });

            peers.insert(addr.ip().to_string(), peer);
        }
    }
}



#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<String>>();

    let node = P2pNode::new(
        String::from(args.get(1).unwrap_or(&"Anon".to_string())),
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



    loop {

        let mut message = String::new();

        std::io::stdin()
            .read_line(&mut message)
            .expect("failed to read from stdin");

        let name = node.username.clone();

        println!("{name}: {message}");

        for peer in node.peers.lock().await.values_mut() {

            peer.send(&message).await;
        }
    }

    tokio::try_join!(listening_task, broadcast_listener_task, broadcast_task).unwrap();
}
