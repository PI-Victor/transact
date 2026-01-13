pub mod engine;
pub mod transaction;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
