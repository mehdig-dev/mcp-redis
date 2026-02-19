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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HashFieldParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(description = "Hash key name")]
    pub key: String,

    #[schemars(description = "Comma-separated field names to retrieve")]
    pub fields: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListRangeParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(description = "List key name")]
    pub key: String,

    #[schemars(description = "Start index (default: 0)")]
    #[serde(default)]
    pub start: Option<i64>,

    #[schemars(description = "Stop index, inclusive (default: -1 for end of list)")]
    #[serde(default)]
    pub stop: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetMembersParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(description = "Set or sorted set key name")]
    pub key: String,

    #[schemars(description = "Maximum number of members to return (for large sets)")]
    #[serde(default)]
    pub count: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SlowlogParams {
    #[schemars(description = "Connection name (optional if only one Redis instance is connected)")]
    #[serde(default)]
    pub connection: Option<String>,

    #[schemars(description = "Number of entries to return (default: 10)")]
    #[serde(default)]
    pub count: Option<u32>,
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

    pub async fn do_get_hash_fields(
        &self,
        params: HashFieldParams,
    ) -> Result<CallToolResult, ErrorData> {
        let entry = self
            .resolve(params.connection.as_deref())
            .map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let fields: Vec<&str> = params.fields.split(',').map(|f| f.trim()).collect();

        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&params.key);
        for field in &fields {
            cmd.arg(*field);
        }

        let values: Vec<Option<String>> = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let result: Vec<serde_json::Value> = fields
            .iter()
            .zip(values.iter())
            .map(|(field, value)| {
                serde_json::json!({
                    "field": field,
                    "value": value.as_deref().unwrap_or_default(),
                    "exists": value.is_some(),
                })
            })
            .collect();

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "key": params.key,
            "fields": result,
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_get_list_range(
        &self,
        params: ListRangeParams,
    ) -> Result<CallToolResult, ErrorData> {
        let entry = self
            .resolve(params.connection.as_deref())
            .map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let start = params.start.unwrap_or(0);
        let stop = params.stop.unwrap_or(-1);

        let elements: Vec<String> = redis::cmd("LRANGE")
            .arg(&params.key)
            .arg(start)
            .arg(stop)
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "key": params.key,
            "start": start,
            "stop": stop,
            "elements": elements,
            "count": elements.len(),
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_get_set_members(
        &self,
        params: SetMembersParams,
    ) -> Result<CallToolResult, ErrorData> {
        let entry = self
            .resolve(params.connection.as_deref())
            .map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        // Detect key type to handle sets vs sorted sets
        let key_type: String = redis::cmd("TYPE")
            .arg(&params.key)
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        match key_type.as_str() {
            "set" => {
                let members: Vec<String> = redis::cmd("SMEMBERS")
                    .arg(&params.key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;

                let limited = if let Some(count) = params.count {
                    members.into_iter().take(count as usize).collect::<Vec<_>>()
                } else {
                    members
                };

                let text = serde_json::to_string_pretty(&serde_json::json!({
                    "key": params.key,
                    "type": "set",
                    "members": limited,
                    "count": limited.len(),
                }))
                .unwrap_or_else(|_| "{}".to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            "zset" => {
                let stop = params.count.map(|c| c - 1).unwrap_or(-1);
                let members: Vec<(String, f64)> = redis::cmd("ZRANGE")
                    .arg(&params.key)
                    .arg(0)
                    .arg(stop)
                    .arg("WITHSCORES")
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(McpRedisError::Redis(e)))?;

                let result: Vec<serde_json::Value> = members
                    .iter()
                    .map(|(m, s)| serde_json::json!({"member": m, "score": s}))
                    .collect();

                let text = serde_json::to_string_pretty(&serde_json::json!({
                    "key": params.key,
                    "type": "zset",
                    "members": result,
                    "count": result.len(),
                }))
                .unwrap_or_else(|_| "{}".to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            "none" => Ok(CallToolResult::success(vec![Content::text(
                serde_json::json!({"error": "Key does not exist", "key": params.key}).to_string(),
            )])),
            other => Ok(CallToolResult::success(vec![Content::text(
                serde_json::json!({"error": format!("Key is type '{}', not a set or zset", other), "key": params.key}).to_string(),
            )])),
        }
    }

    pub async fn do_slowlog(&self, params: SlowlogParams) -> Result<CallToolResult, ErrorData> {
        let entry = self
            .resolve(params.connection.as_deref())
            .map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let count = params.count.unwrap_or(10);

        // SLOWLOG GET returns an array of arrays
        let raw: Vec<Vec<redis::Value>> = redis::cmd("SLOWLOG")
            .arg("GET")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let entries: Vec<serde_json::Value> = raw
            .iter()
            .map(|entry| {
                let id = match entry.first() {
                    Some(redis::Value::Int(i)) => *i,
                    _ => -1,
                };
                let timestamp = match entry.get(1) {
                    Some(redis::Value::Int(i)) => *i,
                    _ => 0,
                };
                let duration_us = match entry.get(2) {
                    Some(redis::Value::Int(i)) => *i,
                    _ => 0,
                };
                let command = match entry.get(3) {
                    Some(redis::Value::Array(args)) => args
                        .iter()
                        .filter_map(|a| match a {
                            redis::Value::BulkString(s) => {
                                String::from_utf8(s.clone()).ok()
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" "),
                    _ => "unknown".to_string(),
                };

                serde_json::json!({
                    "id": id,
                    "timestamp": timestamp,
                    "duration_us": duration_us,
                    "command": command,
                })
            })
            .collect();

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "entries": entries,
            "count": entries.len(),
        }))
        .unwrap_or_else(|_| "{}".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    pub async fn do_client_list(
        &self,
        params: ConnectionParam,
    ) -> Result<CallToolResult, ErrorData> {
        let entry = self
            .resolve(params.connection.as_deref())
            .map_err(|e| self.err(e))?;
        let mut conn = entry.conn.clone();

        let raw: String = redis::cmd("CLIENT")
            .arg("LIST")
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(McpRedisError::Redis(e)))?;

        let clients: Vec<serde_json::Value> = raw
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                let mut map = serde_json::Map::new();
                for part in line.split(' ') {
                    if let Some((key, value)) = part.split_once('=') {
                        map.insert(
                            key.to_string(),
                            serde_json::Value::String(value.to_string()),
                        );
                    }
                }
                serde_json::Value::Object(map)
            })
            .collect();

        let text = serde_json::to_string_pretty(&serde_json::json!({
            "clients": clients,
            "count": clients.len(),
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

    #[tool(
        name = "get_hash_fields",
        description = "Get specific fields from a hash key using HMGET"
    )]
    async fn get_hash_fields(
        &self,
        Parameters(params): Parameters<HashFieldParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_get_hash_fields(params).await
    }

    #[tool(
        name = "get_list_range",
        description = "Get a range of elements from a list key using LRANGE"
    )]
    async fn get_list_range(
        &self,
        Parameters(params): Parameters<ListRangeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_get_list_range(params).await
    }

    #[tool(
        name = "get_set_members",
        description = "Get members of a set (SMEMBERS) or sorted set (ZRANGE with scores)"
    )]
    async fn get_set_members(
        &self,
        Parameters(params): Parameters<SetMembersParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_get_set_members(params).await
    }

    #[tool(
        name = "slowlog",
        description = "Get slow query log entries for performance debugging"
    )]
    async fn slowlog(
        &self,
        Parameters(params): Parameters<SlowlogParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_slowlog(params).await
    }

    #[tool(
        name = "client_list",
        description = "List connected Redis clients with address, name, idle time, and current command"
    )]
    async fn client_list(
        &self,
        Parameters(params): Parameters<ConnectionParam>,
    ) -> Result<CallToolResult, ErrorData> {
        self.do_client_list(params).await
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
                "Redis server. Tools: list_connections (instances), info (server stats), \
                 scan_keys (find keys), get (retrieve values), key_info (metadata), \
                 dbsize (key count), search_keys (keys with types), \
                 get_hash_fields (hash HMGET), get_list_range (list LRANGE), \
                 get_set_members (set/zset members), slowlog (slow queries), \
                 client_list (connected clients)."
                    .to_string(),
            ),
        }
    }
}
