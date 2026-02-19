use anyhow::{bail, Result};
use clap::Parser;
use mcp_redis::server;
use rmcp::{transport::stdio, ServiceExt};
use tracing_subscriber::EnvFilter;

/// MCP server for Redis — lets LLMs explore keys, values, and server info
#[derive(Parser)]
#[command(name = "mcp-redis", version, about)]
struct Cli {
    /// Redis connection URL (repeatable for multiple instances).
    /// Example: redis://127.0.0.1:6379
    #[arg(long = "url")]
    urls: Vec<String>,

    /// Read a Redis URL from an environment variable (repeatable).
    /// Example: --url-env REDIS_URL
    #[arg(long = "url-env")]
    url_envs: Vec<String>,

    /// Allow write operations (SET, DEL, FLUSHDB, etc.).
    /// By default, only read operations are permitted.
    #[arg(long)]
    allow_write: bool,

    /// Number of keys per SCAN iteration (default: 100)
    #[arg(long, default_value = "100")]
    scan_count: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    // Collect URLs from --url and --url-env
    let mut all_urls = cli.urls.clone();

    for env_name in &cli.url_envs {
        match std::env::var(env_name) {
            Ok(url) => {
                tracing::info!(env = env_name, "Read Redis URL from environment variable");
                all_urls.push(url);
            }
            Err(_) => {
                bail!("Environment variable '{env_name}' is not set");
            }
        }
    }

    if all_urls.is_empty() {
        // Default to localhost
        all_urls.push("redis://127.0.0.1:6379".to_string());
        tracing::info!("No URL provided, defaulting to redis://127.0.0.1:6379");
    }

    // Connect to all Redis instances
    let mut connections = Vec::new();
    for (i, url_str) in all_urls.iter().enumerate() {
        let client = redis::Client::open(url_str.as_str())
            .map_err(|e| anyhow::anyhow!("Invalid Redis URL '{}': {}", url_str, e))?;

        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| anyhow::anyhow!("Cannot connect to '{}': {}", url_str, e))?;

        let name = if all_urls.len() == 1 {
            "redis".to_string()
        } else {
            // Extract host:port for meaningful names (like mcp-sql's extract_db_name)
            extract_connection_name(url_str, i)
        };

        // Redact password from URL for display
        let redacted = redact_url(url_str);

        connections.push(server::RedisConnection {
            name,
            url_redacted: redacted,
            conn,
        });

        tracing::info!(url = %redact_url(url_str), "Connected to Redis");
    }

    tracing::info!(
        connections = connections.len(),
        allow_write = cli.allow_write,
        scan_count = cli.scan_count,
        "Starting mcp-redis server"
    );

    let service = server::McpRedisServer::new(connections, cli.allow_write, cli.scan_count);
    let running = service.serve(stdio()).await?;
    running.waiting().await?;

    Ok(())
}

fn extract_connection_name(url_str: &str, index: usize) -> String {
    if let Ok(parsed) = url::Url::parse(url_str) {
        let host = parsed.host_str().unwrap_or("unknown");
        let port = parsed.port().unwrap_or(6379);
        let db = parsed.path().trim_start_matches('/');
        if db.is_empty() || db == "0" {
            format!("{}:{}", host, port)
        } else {
            format!("{}:{}/{}", host, port, db)
        }
    } else {
        format!("redis-{}", index)
    }
}

fn redact_url(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(mut parsed) => {
            if parsed.password().is_some() {
                let _ = parsed.set_password(Some("***"));
            }
            parsed.to_string()
        }
        Err(_) => url_str.to_string(),
    }
}
