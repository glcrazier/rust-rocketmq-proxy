use gmq_remoting::{channel::Channel, common::command::Command};
#[tokio::main]
async fn main() {
    let channel = Channel::new("127.0.0.1:9876").await.unwrap();
    let cmd = Command::new(105);
    
    let response = channel.request(cmd).await;

}