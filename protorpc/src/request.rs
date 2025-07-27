use std::{collections::HashMap, ops::Deref, time::Duration};

/// Default timeout is 5 seconds
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Request<T> {
    pub payload: T,
    pub timeout: Duration,
    pub metadata: HashMap<String, String>,
}

impl<T> Request<T> {
    pub fn into_inner(self) -> T {
        self.payload
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn get_timeout(&self) -> Duration {
        self.timeout
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
    }

    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl<T> Deref for Request<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> AsRef<T> for Request<T> {
    fn as_ref(&self) -> &T {
        &self.payload
    }
}

impl<T> AsMut<T> for Request<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.payload
    }
}

impl<T> From<T> for Request<T> {
    fn from(payload: T) -> Self {
        Self {
            payload,
            timeout: DEFAULT_TIMEOUT,
            metadata: Default::default(),
        }
    }
}
