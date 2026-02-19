//! MCP server that lets LLMs explore Redis keys, values, and server info.
//!
//! Provides tools for scanning keys, reading values of any type (string, hash,
//! list, set, zset), inspecting key metadata, and querying server statistics.

pub mod error;
pub mod server;
