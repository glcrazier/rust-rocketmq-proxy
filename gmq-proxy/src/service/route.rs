use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};

use crate::{common::Error, remoting::client::TopicRouteData};

pub struct RouteService {
    topic_route_cache: Arc<RwLock<HashMap<String, TopicRouteData>>>,
}

impl RouteService {
    pub fn new() -> Self {
        Self {
            topic_route_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn load_route(&self, topic_name: &str) -> Result<TopicRouteData, Error> {
        Ok(TopicRouteData::default())
    }
}
