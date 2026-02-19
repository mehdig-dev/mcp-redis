use mcp_redis::server::{
    ConnectionParam, InfoParams, KeyParams, McpRedisServer, RedisConnection, ScanParams,
};

/// Try to connect to Redis with a short timeout. Skip tests if not available.
async fn try_connect() -> Option<RedisConnection> {
    let url =
        std::env::var("REDIS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string());

    let client = match redis::Client::open(url.as_str()) {
        Ok(c) => c,
        Err(_) => return None,
    };

    // Use a timeout so tests skip quickly when Redis is not running
    let conn = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        redis::aio::ConnectionManager::new(client),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => return None,
    };

    // Verify connection works
    let mut test_conn = conn.clone();
    let pong: Result<String, _> = redis::cmd("PING").query_async(&mut test_conn).await;
    if pong.is_err() {
        return None;
    }

    // Flush DB 15 for clean test state
    let _: Result<(), _> = redis::cmd("FLUSHDB").query_async(&mut test_conn).await;

    Some(RedisConnection {
        name: "test-redis".to_string(),
        url_redacted: "redis://127.0.0.1:6379/15".to_string(),
        conn,
    })
}

/// Connect or skip the test gracefully.
macro_rules! require_redis {
    () => {
        match try_connect().await {
            Some(c) => c,
            None => {
                eprintln!("Skipping: Redis not available");
                return;
            }
        }
    };
}

fn make_server(conn: RedisConnection) -> McpRedisServer {
    McpRedisServer::new(vec![conn], false, 100)
}

fn extract_text(result: rmcp::model::CallToolResult) -> serde_json::Value {
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

#[tokio::test]
async fn test_list_connections() {
    let conn = require_redis!();
    let server = make_server(conn);
    let result = server.do_list_connections().await.expect("list_connections failed");
    let json = extract_text(result);
    let arr = json.as_array().expect("should be array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "test-redis");
}

#[tokio::test]
async fn test_info() {
    let conn = require_redis!();
    let server = make_server(conn);
    let params = InfoParams { connection: None, section: None };
    let result = server.do_info(params).await.expect("info failed");
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default();
    assert!(!text.is_empty(), "INFO should return non-empty string");
    assert!(text.contains("redis_version"), "INFO should contain redis_version");
}

#[tokio::test]
async fn test_info_section() {
    let conn = require_redis!();
    let server = make_server(conn);
    let params = InfoParams { connection: None, section: Some("memory".to_string()) };
    let result = server.do_info(params).await.expect("info section failed");
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default();
    assert!(text.contains("used_memory"), "INFO memory should contain used_memory");
}

#[tokio::test]
async fn test_scan_keys_empty() {
    let conn = require_redis!();
    let server = make_server(conn);
    let params = ScanParams { connection: None, pattern: None, count: None };
    let result = server.do_scan_keys(params).await.expect("scan_keys failed");
    let json = extract_text(result);
    assert_eq!(json["count"], 0, "Empty DB should have 0 keys");
}

#[tokio::test]
async fn test_scan_keys_with_data() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("SET").arg("test:key1").arg("val1").query_async(&mut test_conn).await.unwrap();
    let _: () = redis::cmd("SET").arg("test:key2").arg("val2").query_async(&mut test_conn).await.unwrap();
    let _: () = redis::cmd("SET").arg("other:key").arg("val3").query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);

    // Scan all
    let params = ScanParams { connection: None, pattern: None, count: None };
    let result = server.do_scan_keys(params).await.expect("scan_keys failed");
    let json = extract_text(result);
    assert_eq!(json["count"], 3);

    // Scan with pattern
    let params = ScanParams { connection: None, pattern: Some("test:*".to_string()), count: None };
    let result = server.do_scan_keys(params).await.expect("scan_keys pattern failed");
    let json = extract_text(result);
    assert_eq!(json["count"], 2);
}

#[tokio::test]
async fn test_get_string() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("SET").arg("mystr").arg("hello world").query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);
    let params = KeyParams { connection: None, key: "mystr".to_string() };
    let result = server.do_get(params).await.expect("get failed");
    let json = extract_text(result);
    assert_eq!(json["type"], "string");
    assert_eq!(json["value"], "hello world");
}

#[tokio::test]
async fn test_get_hash() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("HSET")
        .arg("myhash").arg("field1").arg("val1").arg("field2").arg("val2")
        .query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);
    let params = KeyParams { connection: None, key: "myhash".to_string() };
    let result = server.do_get(params).await.expect("get hash failed");
    let json = extract_text(result);
    assert_eq!(json["type"], "hash");
    assert_eq!(json["value"]["field1"], "val1");
    assert_eq!(json["value"]["field2"], "val2");
}

#[tokio::test]
async fn test_get_list() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("RPUSH")
        .arg("mylist").arg("a").arg("b").arg("c")
        .query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);
    let params = KeyParams { connection: None, key: "mylist".to_string() };
    let result = server.do_get(params).await.expect("get list failed");
    let json = extract_text(result);
    assert_eq!(json["type"], "list");
    let values = json["value"].as_array().unwrap();
    assert_eq!(values.len(), 3);
}

#[tokio::test]
async fn test_get_nonexistent() {
    let conn = require_redis!();
    let server = make_server(conn);
    let params = KeyParams { connection: None, key: "does_not_exist".to_string() };
    let result = server.do_get(params).await.expect("get nonexistent failed");
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default();
    assert!(text.contains("Key does not exist"));
}

#[tokio::test]
async fn test_key_info() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("SET").arg("infokey").arg("val").query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);
    let params = KeyParams { connection: None, key: "infokey".to_string() };
    let result = server.do_key_info(params).await.expect("key_info failed");
    let json = extract_text(result);
    assert_eq!(json["type"], "string");
    assert_eq!(json["ttl"], "no expiry");
}

#[tokio::test]
async fn test_dbsize() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("SET").arg("k1").arg("v1").query_async(&mut test_conn).await.unwrap();
    let _: () = redis::cmd("SET").arg("k2").arg("v2").query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);
    let params = ConnectionParam { connection: None };
    let result = server.do_dbsize(params).await.expect("dbsize failed");
    let json = extract_text(result);
    assert_eq!(json["dbsize"], 2);
}

#[tokio::test]
async fn test_search_keys_pipeline() {
    let conn = require_redis!();
    let mut test_conn = conn.conn.clone();
    let _: () = redis::cmd("SET").arg("search:str").arg("val").query_async(&mut test_conn).await.unwrap();
    let _: () = redis::cmd("HSET").arg("search:hash").arg("f").arg("v").query_async(&mut test_conn).await.unwrap();

    let server = make_server(conn);
    let params = ScanParams { connection: None, pattern: Some("search:*".to_string()), count: None };
    let result = server.do_search_keys(params).await.expect("search_keys failed");
    let json = extract_text(result);
    assert_eq!(json["count"], 2);

    // Verify types were returned (via pipeline)
    let keys = json["keys"].as_array().unwrap();
    let types: Vec<&str> = keys.iter().map(|k| k["type"].as_str().unwrap()).collect();
    assert!(types.contains(&"string"));
    assert!(types.contains(&"hash"));
}

#[tokio::test]
async fn test_resolve_ambiguous() {
    let conn = require_redis!();
    // Create a second connection (clone of the first) with a different name
    let conn2 = RedisConnection {
        name: "test-redis-2".to_string(),
        url_redacted: conn.url_redacted.clone(),
        conn: conn.conn.clone(),
    };
    let server = McpRedisServer::new(vec![conn, conn2], false, 100);

    // With two connections, list should show both
    let result = server.do_list_connections().await.expect("list_connections failed");
    let json = extract_text(result);
    let arr = json.as_array().expect("should be array");
    assert_eq!(arr.len(), 2);
}
