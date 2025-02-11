use gmq_proxy::{remoting::client::MQClient, service::server::GrpcMessagingServer};

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let mut mq_client = MQClient::new("127.0.0.1:8080");
    let _ = mq_client.start();
    let result = mq_client.query_route("bbb").await;
    println!("result: {:?}", result);

    
}