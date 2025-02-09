use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The topic {0} does not exist, reason: {1}")]
    TopicNotFound(String, anyhow::Error),
    #[error(transparent)]
    InternalError(#[from] anyhow::Error),
}

#[repr(u8)]
pub enum RequestCode {
    GetTopicRouteInfo = 105,
}
