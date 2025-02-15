use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use parking_lot::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt}, net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpSocket, TcpStream,
    }, select, sync::{mpsc, oneshot}, task::{block_in_place, spawn_blocking}, time::timeout
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{common::command::Command, util::Error};

#[derive(Debug)]
pub struct Channel {
    command_sender: mpsc::Sender<Request>,
    timeout: Duration,
    shutdown_token: CancellationToken,
    shutdown_tracker: TaskTracker,
}

struct Request {
    cmd: Command,
    response_tx: oneshot::Sender<Result<Command, Error>>,
}

/**
 * A channel sends and receives Command messages.
 */
impl Channel {
    pub async fn new(addr: &str) -> Result<Self, Error> {
        let addr = addr
            .parse()
            .map_err(|_| Error::InvalidAddress(addr.to_string()))?;
        let (tx, mut request_rx) = mpsc::channel(1024);
        let command_sender: mpsc::Sender<Request> = tx;
        let response_table: Arc<RwLock<HashMap<usize, oneshot::Sender<Result<Command, Error>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let token = CancellationToken::new();

        let (create_stream_tx, mut create_stream_rx) = mpsc::channel(32);
        let create_stream_tx: mpsc::Sender<()> = create_stream_tx;

        let (reader_tx, mut reader_rx) = mpsc::channel(32);
        let reader_tx: mpsc::Sender<OwnedReadHalf> = reader_tx;
        let (writer_tx, mut writer_rx) = mpsc::channel(32);
        let writer_tx: mpsc::Sender<OwnedWriteHalf> = writer_tx;

        let shutdown_token = token.clone();

        let task_tracker = TaskTracker::new();
        task_tracker.spawn(async move {
            loop {
                select! {
                    _ = create_stream_rx.recv() => {
                        let result = Channel::new_stream(addr).await;
                        if let Ok(stream) = result {
                            let (reader, writer) = stream.into_split();
                            let _reader = reader_tx.send(reader).await;
                            let _writer = writer_tx.send(writer).await;
                        } else {
                            println!("create stream failed: {:?}", result);
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        create_stream_tx
            .send(())
            .await
            .map_err(|e| Error::InternalError(e.into()))?;

        let response_table_for_reader = Arc::clone(&response_table);
        let shutdown_token_2 = token.clone();
        let create_stream_tx_2 = create_stream_tx.clone();

        task_tracker.spawn(async move {
            loop {
                select! {
                    Some(mut reader) = reader_rx.recv() => {
                        loop {
                            if shutdown_token_2.is_cancelled() {
                                println!("shutdown reader loop");
                                break;
                            }
                            match Channel::read_command(&mut reader).await {
                                Ok(command) => {
                                    // send out command
                                    let opaque = command.opaque();
                                    if let Some(sender) = response_table_for_reader.write().remove(&opaque) {
                                        if let Err(e) = sender.send(Ok(command)) {
                                            println!("Send response command error: {:?}", e);
                                        }
                                    } else {
                                        println!("no entry of {} in response table, drop command", opaque);
                                    }
                                }
                                Err(Error::TryLater) => {
                                    // try later
                                }
                                Err(e) => {
                                    println!("read stream encounters error {:?}", e);
                                    let _ = create_stream_tx.send(()).await;
                                    break;
                                }
                            }

                        }
                    }
                    _ = shutdown_token_2.cancelled() => {
                        println!("shutdown reader task");
                        break;
                    }
                }
            }
        });

        let response_table_for_writer = Arc::clone(&response_table);
        let shutdown_token3 = token.clone();

        task_tracker.spawn(async move {
            loop {
                select! {
                    Some(mut writer) = writer_rx.recv() => {
                        loop {
                            select! {
                                Some(request) = request_rx.recv() => {
                                    let opaque = request.cmd.opaque();
                                    match Channel::write_command(&mut writer, request.cmd).await {
                                        Ok(_) => {
                                           response_table_for_writer.write().insert(opaque, request.response_tx);
                                        }
                                        Err(e) => {
                                            let _ = request.response_tx.send(Err(e));
                                            let _x = create_stream_tx_2.send(()).await;
                                            break;
                                        }
                                    }
                                }
                                _ = shutdown_token3.cancelled() => {
                                    println!("shutdown write loop");
                                    break;
                                }
                            }
                        }
                    }
                    _ = shutdown_token3.cancelled() => {
                        println!("shutdown writer task");
                        break;
                    }
                }
            }
        });

        task_tracker.close();

        Ok(Self {
            command_sender,
            timeout: Duration::from_secs(10),
            shutdown_token: token.clone(),
            shutdown_tracker: task_tracker,
        })
    }

    async fn new_stream(addr: SocketAddr) -> Result<TcpStream, Error> {
        println!("create new stream with {:?}", addr);
        let socket = TcpSocket::new_v4()?;
        socket.set_nodelay(true)?;
        let stream = socket.connect(addr).await?;
        Ok(stream)
    }

    async fn read_command(reader: &mut OwnedReadHalf) -> Result<Command, Error> {
        let mut frame_length_bytes = [0; 4];
        if reader.peek(&mut frame_length_bytes).await? != 4 {
            return Err(Error::TryLater);
        }
        let frame_length = 4 + u32::from_be_bytes(frame_length_bytes) as usize;
        let mut buf: Vec<u8> = Vec::with_capacity(frame_length);

        while buf.len() < frame_length {
            reader.read_buf(&mut buf).await?;
        }

        return Command::decode_vec(buf);
    }

    async fn write_command(writer: &mut OwnedWriteHalf, cmd: Command) -> Result<(), Error> {
        let mut raw_data = cmd.encode();
        let _ = writer
            .write_all(&mut raw_data)
            .await
            .map_err(|e| Error::WriteError(e.into()))?;
        Ok(())
    }

    pub async fn request(&self, cmd: Command) -> Result<Command, Error> {
        let (response_tx, response_rx) = oneshot::channel();
        let request = Request { cmd, response_tx };
        let result = self.command_sender.try_send(request);
        if let Err(e) = result {
            return Err(Error::WriteError(e.into()));
        }
        match timeout(self.timeout, response_rx).await {
            Ok(response) => match response {
                Ok(Ok(command)) => Ok(command),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(Error::ReadError),
            },
            Err(_) => Err(Error::Timeout),
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        self.shutdown_tracker.wait().await;
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
    
}