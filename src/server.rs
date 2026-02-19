use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{schemars, tool, tool_handler, tool_router, ServerHandler};
use serde::Deserialize;

use crate::error::McpRedisError;

/// Maximum number of SCAN iterations as a safety valve
const MAX_SCAN_ITERATIONS: usize = 1000;

#[derive(Clone)]
pub struct RedisConnection {
    pub name: String,
    pub url_redacted: String,
    pub conn: redis::aio::ConnectionManager,
}

#[derive(Clone)]
pub struct McpRedisServer {
    connections: Arc<Vec<RedisConnection>>,
    allow_write: bool,
    scan_count: u32,
    tool_router: ToolRouter<Self>,
}

// -- Tool parameter types --

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ConnectionParam {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InfoParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(
        description = "Info section to retrieve (e.g. 'memory', 'stats', 'keyspace', 'server'). Default: all"
    )]
    #[serde(default)]
    pub section: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ScanParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(description = "Key pattern to match (e.g. 'user:*', 'session:*'). Default: *")]
    #[serde(default)]
    pub pattern: Option<String>,

    #[schemars(description = "Maximum number of keys to return")]
    #[serde(default)]
    pub count: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct KeyParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(description = "Key name to inspect")]
    pub key: String,
}

impl McpRedisServer {
    pub fn new(connections: Vec<RedisConnection>, allow_write: bool, scan_count: u32) -> Self {
        Self {
            connections: Arc::new(connections),
            allow_write,
            scan_count,
            tool_router: Self::tool_router(),
        }
    }

    fn resolve(&self, name: Option<&str>) -> Result<&RedisConnection, McpRedisError> {
        match name {
            Some(n) => self
                .connections
                .iter()
                .find(|c| c.name == n)
                .ok_or_else(|| McpRedisError::ConnectionNotFound(n.to_string())),
            None if self.connections.len() == 1 => Ok(&self.connections[0]),
            None => Err(McpRedisError::AmbiguousConnection),
        }
    }

    /// Guard for future write operations. Currently all tools are read-only,
    /// but any new write tools should call this first.
    #[allow(dead_code)]
    fn check_read_only(&self, operation: &str) -> Result<(), McpRedisError> {
        if !self.allow_write {
            return Err(McpRedisError::ReadOnly(format!(
                "'{}' requires --allow-write flag",
                operation
            )));
        }
        Ok(())
    }

    /// Validate that a pattern doesn't contain null bytes.
    fn validate_pattern(pattern: &str) -> Result<(), McpRedisError> {
        if pattern.contains('\0') {
            return Err(McpRedisError::Other(
                "Pattern must not contain null bytes".to_string(),
            ));
        }
        Ok(())
    }

    fn err(&self, e: McpRedisError) -> ErrorData {
        e.to_mcp_error()
    }
}

// -- Read-only Redis commands for reference --
// INFO, SCAN, TYPE, GET, LRANGE, SMEMBERS, ZRANGE, HGETALL, TTL, OBJECT, MEMORY, DBSIZE
// Write commands that would need check_read_only: SET, DEL, FLUSHDB, EXPIRE, etc.

// -- Public methods for testability --

impl McpRedisServer {
    pub async fn do_list_connections(&self) -> Result<CallToolResult, ErrorData> {
        let connections: Vec<serde_json::Value> = self
            .connections
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "url": c.url_redacted,
                })
            })
            .collect();

        let text =
            serde_json::to_string_pretty(&connections).unwrap_or_else(|_| "[]".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_info(&self, params: InfoParams) -> Result<CallToolResult, ErrorData> {
        let entry = self.resolve(params.connection.as_deref()).map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let info: String = if let Some(section) = params.section {
            redis::cmd("INFO")
                .arg(&section)
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(McpRedisError::Redis(e)))?
        } else {
            redis::cmd("INFO")
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(McpRedisError::Redis(e)))?
        };

        Ok(CallToolResult::success(vec![Content::text(info)]))
    }

    pub async fn do_scan_keys(&self, params: ScanParams) -> Result<CallToolResult, ErrorData> {
        let entry = self.resolve(params.connection.as_deref()).map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();
        let pattern = params.pattern.as_deref().unwrap_or("*");

        Self::validate_pattern(pattern).map_err(|e| self.err(e))?;

        // Cap max_keys to scan_count to prevent unbounded iteration
        let max_keys = std::cmp::min(
            params.count.unwrap_or(self.scan_count) as usize,
            self.scan_count as usize,
        );

        let mut keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let mut iterations = 0;

        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(McpRedisError::Redis(e)))?;

            keys.extend(batch);
            cursor = next_cursor;
            iterations += 1;

            if cursor == 0 || keys.len() >= max_keys || iterations >= MAX_SCAN_ITERATIONS {
                break;
            }
        }

        keys.truncate(max_keys);

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "pattern": pattern,
            "keys": keys,
            "count": keys.len(),
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_get(&self, params: KeyParams) -> Result<CallToolResult, ErrorData> {
        let entry = self.resolve(params.connection.as_deref()).map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        // Get key type first
        let key_type: String = redis::cmd("TYPE")
            .arg(&params.key)
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let value: serde_json::Value = match key_type.as_str() {
            "string" => {
                let v: String = redis::cmd("GET")
                    .arg(&params.key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;
                serde_json::Value::String(v)
            }
            "list" => {
                let v: Vec<String> = redis::cmd("LRANGE")
                    .arg(&params.key)
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;
                serde_json::json!(v)
            }
            "set" => {
                let v: Vec<String> = redis::cmd("SMEMBERS")
                    .arg(&params.key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;
                serde_json::json!(v)
            }
            "zset" => {
                let v: Vec<(String, f64)> = redis::cmd("ZRANGE")
                    .arg(&params.key)
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;
                serde_json::json!(v.iter().map(|(m, s)| serde_json::json!({"member": m, "score": s})).collect::<Vec<_>>())
            }
            "hash" => {
                let v: Vec<(String, String)> = redis::cmd("HGETALL")
                    .arg(&params.key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;
                let map: serde_json::Map<String, serde_json::Value> = v
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect();
                serde_json::Value::Object(map)
            }
            "none" => {
                return Ok(CallToolResult::success(vec![Content::text(
                    serde_json::json!({"error": "Key does not exist", "key": params.key}).to_string(),
                )]));
            }
            other => serde_json::json!({"type": other, "note": "Unsupported type"}),
        };

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "key": params.key,
            "type": key_type,
            "value": value,
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_key_info(&self, params: KeyParams) -> Result<CallToolResult, ErrorData> {
        let entry = self.resolve(params.connection.as_deref()).map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let key_type: String = redis::cmd("TYPE")
            .arg(&params.key)
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let ttl: i64 = redis::cmd("TTL")
            .arg(&params.key)
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let encoding: Result<String, _> = redis::cmd("OBJECT")
            .arg("ENCODING")
            .arg(&params.key)
            .query_async(&mut conn)
            .await;

        let memory: Result<i64, _> = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg(&params.key)
            .query_async(&mut conn)
            .await;

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "key": params.key,
            "type": key_type,
            "ttl": if ttl == -1 { "no expiry".to_string() } else if ttl == -2 { "key not found".to_string() } else { format!("{}s", ttl) },
            "encoding": encoding.unwrap_or_else(|_| "unknown".to_string()),
            "memory_bytes": memory.unwrap_or(-1),
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_dbsize(&self, params: ConnectionParam) -> Result<CallToolResult, ErrorData> {
        let entry = self.resolve(params.connection.as_deref()).map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let size: i64 = redis::cmd("DBSIZE")
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "dbsize": size,
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_search_keys(&self, params: ScanParams) -> Result<CallToolResult, ErrorData> {
        let entry = self.resolve(params.connection.as_deref()).map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();
        let pattern = params.pattern.as_deref().unwrap_or("*");

        Self::validate_pattern(pattern).map_err(|e| self.err(e))?;

        // Cap max_keys to scan_count to prevent unbounded iteration
        let max_keys = std::cmp::min(
            params.count.unwrap_or(self.scan_count) as usize,
            self.scan_count as usize,
        );

        let mut keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let mut iterations = 0;

        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(McpRedisError::Redis(e)))?;

            keys.extend(batch);
            cursor = next_cursor;
            iterations += 1;

            if cursor == 0 || keys.len() >= max_keys || iterations >= MAX_SCAN_ITERATIONS {
                break;
            }
        }

        keys.truncate(max_keys);

        // Batch TYPE queries using a pipeline instead of N+1 individual calls
        let mut results = Vec::new();
        if !keys.is_empty() {
            let mut pipe = redis::pipe();
            for key in &keys {
                pipe.cmd("TYPE").arg(key);
            }
            let types: Vec<String> = pipe
                .query_async(&mut conn)
                .await
                .unwrap_or_else(|_| vec!["unknown".to_string(); keys.len()]);

            for (key, key_type) in keys.iter().zip(types.iter()) {
                results.push(serde_json::json!({
                    "key": key,
                    "type": key_type,
                }));
            }
        }

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "pattern": pattern,
            "keys": results,
            "count": results.len(),
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }
}

// -- MCP tool handlers (thin wrappers) --

#[tool_router]
impl McpRedisServer {
    #[tool(
        name = "list_connections",
        description = "List all connected Redis instances with names and connection info (passwords redacted)"
    )]
    async fn list_connections(&self) -> Result<CallToolResult, ErrorData> {
        self.do_list_connections().await
    }

    #[tool(
        name = "info",
        description = "Get Redis server info. Optionally specify a section: memory, stats, keyspace, server, clients, etc."
    )]
    async fn info(
        &self,
        Parameters(params): Parameters<InfoParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_info(params).await
    }

    #[tool(
        name = "scan_keys",
        description = "Scan keys matching a pattern using SCAN (non-blocking). Returns key names."
    )]
    async fn scan_keys(
        &self,
        Parameters(params): Parameters<ScanParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_scan_keys(params).await
    }

    #[tool(
        name = "get",
        description = "Get the value of a key. Auto-detects the key type (string, hash, list, set, zset) and returns the appropriate representation."
    )]
    async fn get(
        &self,
        Parameters(params): Parameters<KeyParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_get(params).await
    }

    #[tool(
        name = "key_info",
        description = "Get metadata about a key: type, TTL, encoding, and memory usage"
    )]
    async fn key_info(
        &self,
        Parameters(params): Parameters<KeyParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_key_info(params).await
    }

    #[tool(
        name = "dbsize",
        description = "Get the number of keys in the current database"
    )]
    async fn dbsize(
        &self,
        Parameters(params): Parameters<ConnectionParam>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_dbsize(params).await
    }

    #[tool(
        name = "search_keys",
        description = "Scan keys matching a pattern and return each key with its type"
    )]
    async fn search_keys(
        &self,
        Parameters(params): Parameters<ScanParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_search_keys(params).await
    }
}

#[tool_handler]
impl ServerHandler for McpRedisServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "mcp-redis".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                ..Default::default()
            },
            instructions: Some(
                "Redis server. Use list_connections to see connected instances, \
                 info for server statistics, scan_keys to find keys by pattern, \
                 get to retrieve values, key_info for metadata, and dbsize for key count."
                    .to_string(),
            ),
        }
    }
}
