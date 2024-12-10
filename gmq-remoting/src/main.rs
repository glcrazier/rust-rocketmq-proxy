use gmq_remoting::{client::Channel, common::command::Command};
#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let channel = Channel::new("127.0.0.1:9876").await.unwrap();
    let cmd = Command::new(105);
    
    let response = channel.request(cmd).await;

}