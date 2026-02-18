# mcp-redis

MCP server that lets LLMs interact with Redis and Valkey databases. Single binary, read-only by default, multi-instance.

## Install

```bash
cargo install mcp-redis
```

## Usage

```bash
# Default (localhost:6379)
mcp-redis

# Specific instance
mcp-redis --url redis://myhost:6379

# With password
mcp-redis --url redis://:password@host:6379

# Multiple instances
mcp-redis --url redis://host1:6379 --url redis://host2:6379

# From environment variable
mcp-redis --url-env REDIS_URL

# Enable write operations
mcp-redis --url redis://host:6379 --allow-write
```

## Configuration

### Claude Code

```bash
claude mcp add redis -- mcp-redis --url redis://localhost:6379
```

### Claude Desktop

```json
{
  "mcpServers": {
    "redis": {
      "command": "mcp-redis",
      "args": ["--url", "redis://localhost:6379"]
    }
  }
}
```

### Cursor / VS Code

```json
{
  "mcpServers": {
    "redis": {
      "command": "mcp-redis",
      "args": ["--url", "redis://localhost:6379"]
    }
  }
}
```

## Tools

| Tool | Description |
|------|-------------|
| `list_connections` | Show all connected Redis instances (passwords redacted) |
| `info` | Get Redis server info (memory, stats, keyspace, etc.) |
| `scan_keys` | Scan keys matching a pattern using SCAN (non-blocking) |
| `get` | Get key value, auto-detecting type (string/hash/list/set/zset) |
| `key_info` | Get key metadata: type, TTL, encoding, memory usage |
| `dbsize` | Get number of keys in the current database |
| `search_keys` | Scan keys with pattern and return keys with their types |

All tools accept an optional `connection` parameter when multiple instances are connected.

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `redis://127.0.0.1:6379` | Redis connection URL (repeatable) |
| `--url-env` | — | Read Redis URL from environment variable (repeatable) |
| `--allow-write` | `false` | Enable write operations |
| `--scan-count` | `100` | Max keys per SCAN iteration |

## Safety

- **Read-only by default** — only read commands are allowed
- **SCAN over KEYS** — uses non-blocking SCAN to avoid blocking Redis
- **Credentials redacted** — passwords are masked in `list_connections` output
- **Connection pooling** — uses ConnectionManager for automatic reconnection

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
