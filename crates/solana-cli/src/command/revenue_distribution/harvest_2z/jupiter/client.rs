use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, header};

pub const JUPITER_API_BASE_URL: &str = "https://api.jup.ag";

#[derive(Debug, Clone)]
pub struct JupiterClient {
    client: Client,
    base_url: String,
}

impl JupiterClient {
    pub fn new(api_key: &str) -> Result<Self> {
        let base_url = std::env::var("JUPITER_API_BASE_URL")
            .unwrap_or_else(|_| JUPITER_API_BASE_URL.to_string());

        Self::with_base_url(api_key, base_url)
    }

    pub fn with_base_url(api_key: &str, base_url: String) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            "x-api-key",
            header::HeaderValue::from_str(api_key).context("Invalid Jupiter API key format")?,
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self { client, base_url })
    }

    #[allow(dead_code)]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &impl serde::Serialize,
    ) -> Result<T> {
        self.execute_with_retry(|| async {
            let url = format!("{}{}", self.base_url, path);
            self.client.get(&url).query(query).send().await
        })
        .await
    }

    pub async fn post<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T> {
        self.execute_with_retry(|| async {
            let url = format!("{}{}", self.base_url, path);
            self.client.post(&url).json(body).send().await
        })
        .await
    }

    async fn execute_with_retry<T, F, Fut>(&self, request_fn: F) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = reqwest::Result<reqwest::Response>>,
    {
        const MAX_RETRIES: u32 = 5;
        const INITIAL_DELAY_MS: u64 = 100;
        const MAX_DELAY_MS: u64 = 5000;

        let mut attempt = 0;
        let mut delay_ms = INITIAL_DELAY_MS;

        loop {
            attempt += 1;

            let response = match request_fn().await {
                Ok(resp) => resp,
                Err(e) => {
                    if attempt >= MAX_RETRIES {
                        bail!("Jupiter API request failed after {MAX_RETRIES} attempts: {e}");
                    }
                    tracing::warn!(
                        "Jupiter API request failed (attempt {attempt}/{MAX_RETRIES}), \
                         retrying in {delay_ms}ms: {e}"
                    );
                    Self::sleep_with_jitter(delay_ms).await;
                    delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                    continue;
                }
            };

            let status = response.status();

            if status.is_success() {
                return response
                    .json()
                    .await
                    .context("Failed to parse Jupiter API response");
            }

            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unable to read body>".to_string());
            let body_snippet = if body.len() > 200 {
                format!("{}...", &body[..200])
            } else {
                body
            };

            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                bail!(
                    "Jupiter API authentication failed (HTTP {status}): {body_snippet}\n\
                     Hint: Check that your JUPITER_API_KEY is valid."
                );
            }

            let is_retryable = matches!(
                status,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
            ) || status.is_server_error();

            if !is_retryable || attempt >= MAX_RETRIES {
                bail!("Jupiter API request failed (HTTP {status}): {body_snippet}");
            }

            tracing::warn!(
                "Jupiter API returned {status} (attempt {attempt}/{MAX_RETRIES}), \
                 retrying in {delay_ms}ms"
            );
            Self::sleep_with_jitter(delay_ms).await;
            delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
        }
    }

    async fn sleep_with_jitter(base_ms: u64) {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::hash::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        std::time::Instant::now().hash(&mut hasher);
        let hash = hasher.finish();

        let jitter_factor = 0.75 + (hash % 50) as f64 / 100.0;
        let delay_ms = (base_ms as f64 * jitter_factor) as u64;

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

#[allow(dead_code)]
pub fn load_api_key_from_env() -> Result<String> {
    std::env::var("JUPITER_API_KEY").map_err(|_| {
        anyhow::anyhow!("Jupiter API key required. Set JUPITER_API_KEY environment variable")
    })
}

#[cfg(test)]
mod tests {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

    use super::*;

    #[tokio::test]
    async fn test_client_sends_api_key_header() {
        let mock_server = MockServer::start().await;

        Mock::given(matchers::method("GET"))
            .and(matchers::path("/swap/v1/quote"))
            .and(matchers::header("x-api-key", "test-api-key-123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": "ok"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = JupiterClient::with_base_url("test-api-key-123", mock_server.uri()).unwrap();

        #[derive(serde::Serialize)]
        struct Query {
            amount: u64,
        }

        #[derive(serde::Deserialize)]
        struct Response {
            data: String,
        }

        let result: Response = client
            .get("/swap/v1/quote", &Query { amount: 1000 })
            .await
            .unwrap();

        assert_eq!(result.data, "ok");
    }

    #[tokio::test]
    async fn test_client_uses_correct_base_url() {
        let mock_server = MockServer::start().await;

        Mock::given(matchers::method("POST"))
            .and(matchers::path("/swap/v1/swap-instructions"))
            .and(matchers::header("x-api-key", "my-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = JupiterClient::with_base_url("my-key", mock_server.uri()).unwrap();

        assert_eq!(client.base_url(), mock_server.uri());
        assert!(!client.base_url().contains("lite-api"));

        #[derive(serde::Serialize)]
        struct Body {
            user: String,
        }

        #[derive(serde::Deserialize)]
        struct Response {
            success: bool,
        }

        let result: Response = client
            .post(
                "/swap/v1/swap-instructions",
                &Body {
                    user: "test".to_string(),
                },
            )
            .await
            .unwrap();

        assert!(result.success);
    }

    #[tokio::test]
    async fn test_missing_api_key_returns_error() {
        let original = std::env::var("JUPITER_API_KEY").ok();
        unsafe { std::env::remove_var("JUPITER_API_KEY") };

        let result = load_api_key_from_env();
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Jupiter API key required"));
        assert!(err_msg.contains("JUPITER_API_KEY"));

        if let Some(val) = original {
            unsafe { std::env::set_var("JUPITER_API_KEY", val) };
        }
    }

    #[tokio::test]
    async fn test_401_error_includes_helpful_message() {
        let mock_server = MockServer::start().await;

        Mock::given(matchers::any())
            .respond_with(
                ResponseTemplate::new(401).set_body_string(r#"{"error": "Invalid API key"}"#),
            )
            .mount(&mock_server)
            .await;

        let client = JupiterClient::with_base_url("bad-key", mock_server.uri()).unwrap();

        #[derive(serde::Serialize)]
        struct Query {}

        let result: Result<serde_json::Value> = client.get("/test", &Query {}).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("401"));
        assert!(err_msg.contains("authentication failed"));
        assert!(err_msg.contains("JUPITER_API_KEY"));
    }

    #[tokio::test]
    async fn test_retries_on_5xx() {
        let mock_server = MockServer::start().await;

        Mock::given(matchers::any())
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal error"))
            .mount(&mock_server)
            .await;

        let client = JupiterClient::with_base_url("key", mock_server.uri()).unwrap();

        #[derive(serde::Serialize)]
        struct Query {}

        let result: Result<serde_json::Value> = client.get("/test", &Query {}).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("500"));
    }

    #[tokio::test]
    async fn test_does_not_retry_on_400() {
        let mock_server = MockServer::start().await;

        Mock::given(matchers::any())
            .respond_with(ResponseTemplate::new(400).set_body_string("Bad request"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = JupiterClient::with_base_url("key", mock_server.uri()).unwrap();

        #[derive(serde::Serialize)]
        struct Query {}

        let result: Result<serde_json::Value> = client.get("/test", &Query {}).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("400"));
    }

    #[test]
    fn test_default_base_url_is_api_jup_ag() {
        assert_eq!(JUPITER_API_BASE_URL, "https://api.jup.ag");
        assert!(!JUPITER_API_BASE_URL.contains("lite-api"));
    }
}
