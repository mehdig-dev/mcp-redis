use rmcp::model::ErrorData;

#[derive(Debug, thiserror::Error)]
pub enum McpRedisError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    #[error("Ambiguous connection: multiple Redis instances connected, specify 'connection' parameter")]
    AmbiguousConnection,

    #[error("Write operation rejected: {0}")]
    ReadOnly(String),

    #[error("{0}")]
    Other(String),
}

impl McpRedisError {
    pub fn to_mcp_error(&self) -> ErrorData {
        match self {
            McpRedisError::ConnectionNotFound(_) | McpRedisError::AmbiguousConnection => {
                ErrorData::invalid_params(self.to_string(), None)
            }
            McpRedisError::ReadOnly(_) => ErrorData::invalid_params(self.to_string(), None),
            McpRedisError::Redis(_) | McpRedisError::Other(_) => {
                ErrorData::internal_error(self.to_string(), None)
            }
        }
    }
}
