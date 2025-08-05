use async_trait::async_trait;
use bytes::Bytes;
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use url::Url;

use anyhow::{anyhow, Result as AnyhowResult};

// Import ObjectStore types
use crate::object_store::{
    path::Path, Attributes, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult,
};
use futures::stream::BoxStream;

#[derive(Debug, Clone)]
pub struct FilesApiHttpClient {
    client: Client,
    workspace_url: String,
    auth_headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub name: String,
    pub is_directory: bool,
    pub file_size: Option<u64>,
    pub last_modified: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct DirectoryListResponse {
    pub contents: Vec<FileInfo>,
}

impl FilesApiHttpClient {
    // pub fn try_new(workspace_url: &str, auth_token: &str) -> AnyhowResult<Self> {
    //     let mut auth_headers = HashMap::new();
    //     auth_headers.insert(
    //         "Authorization".to_string(),
    //         format!("Bearer {}", auth_token),
    //     );

    //     let client = Client::builder()
    //         .timeout(Duration::from_secs(300)) // 5 minute timeout
    //         .build()?;

    //     Ok(Self {
    //         client,
    //         workspace_url: workspace_url.trim_end_matches('/').to_string(),
    //         auth_headers,
    //     })
    // }

    pub fn try_new(workspace_url: &str, user_id: &str, user_name: &str, org_id: &str, account_id: &str) -> AnyhowResult<Self> {
        let mut auth_headers = HashMap::new();
        auth_headers.insert(
            "X-Databricks-User-Id".to_string(), 
            user_id.to_string()
        );
        auth_headers.insert(
            "X-Databricks-User-Name".to_string(), 
            user_name.to_string()
        );
        auth_headers.insert(
            "X-Databricks-Org-Id".to_string(), 
            org_id.to_string()
        );
        auth_headers.insert(
            "X-Databricks-Account-Id".to_string(), 
            account_id.to_string()
        );

        let client = Client::builder()
            .timeout(Duration::from_secs(300)) // 5 minute timeout
            .build()?;

        Ok(Self {
            client,
            workspace_url: workspace_url.to_string(), 
            auth_headers,
        })
    }

    pub async fn get_file(&self, path: &str) -> AnyhowResult<Bytes> {
        let url = self.get_files_url(path);

        let response = self
            .client
            .get(&url)
            .headers(self.build_headers(None)?)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK
            | StatusCode::CREATED
            | StatusCode::NO_CONTENT
            | StatusCode::PARTIAL_CONTENT => Ok(response.bytes().await?),
            StatusCode::TOO_MANY_REQUESTS => Err(anyhow!(
                "Rate limited (429). Consider implementing retry logic."
            )),
            StatusCode::UNAUTHORIZED => {
                Err(anyhow!("Authentication failed (401). Check your token."))
            }
            StatusCode::FORBIDDEN => Err(anyhow!("Access forbidden (403). Check permissions.")),
            StatusCode::NOT_FOUND => Err(anyhow!("Resource not found (404).")),
            StatusCode::INTERNAL_SERVER_ERROR => Err(anyhow!(
                "Internal Server Error (500): {}",
                response.text().await?
            )),
            status => Err(anyhow!("HTTP error {} for URL: {}", status, response.url())),
        }
    }

    pub async fn list_directory(&self, path: &str) -> AnyhowResult<DirectoryListResponse> {
        let url = self.get_directories_url(path);

        let response = self
            .client
            .get(&url)
            .headers(self.build_headers(None)?)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK
            | StatusCode::CREATED
            | StatusCode::NO_CONTENT
            | StatusCode::PARTIAL_CONTENT => {
                let directory_listing: DirectoryListResponse = response.json().await?;
                Ok(directory_listing)
            }
            StatusCode::TOO_MANY_REQUESTS => Err(anyhow!(
                "Rate limited (429). Consider implementing retry logic."
            )),
            StatusCode::UNAUTHORIZED => {
                Err(anyhow!("Authentication failed (401). Check your token."))
            }
            StatusCode::FORBIDDEN => Err(anyhow!("Access forbidden (403). Check permissions.")),
            StatusCode::NOT_FOUND => Err(anyhow!("Resource not found (404).")),
            StatusCode::INTERNAL_SERVER_ERROR => Err(anyhow!(
                "Internal Server Error (500): {}",
                response.text().await?
            )),
            status => Err(anyhow!("HTTP error {} for URL: {}", status, response.url())),
        }
    }

    pub async fn get_head(&self, path: &str) -> AnyhowResult<ObjectMeta> {
        let url = self.get_files_url(path);

        let response = self
            .client
            .head(&url) // Use HEAD instead of GET
            .headers(self.build_headers(None)?)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {
                let headers = response.headers();

                // Parse content-length
                let size = headers
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                // Parse last-modified
                let last_modified = headers
                    .get("last-modified")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|date_str| {
                        // Parse HTTP date format: "Thu, 31 Jul 2025 20:34:04 GMT"
                        chrono::DateTime::parse_from_rfc2822(date_str).ok()
                    })
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|| chrono::Utc::now());

                // Parse e_tag if present
                let e_tag = headers
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                let location = Path::parse(path)?;

                Ok(ObjectMeta {
                    location,
                    last_modified,
                    size,
                    e_tag,
                    version: None,
                })
            }
            StatusCode::NOT_FOUND => Err(anyhow!("File not found (404): {}", path)),
            StatusCode::UNAUTHORIZED => {
                Err(anyhow!("Authentication failed (401). Check your token."))
            }
            StatusCode::FORBIDDEN => Err(anyhow!("Access forbidden (403). Check permissions.")),
            status => Err(anyhow!("HTTP error {} for URL: {}", status, url)),
        }
    }

    fn get_files_url(&self, path: &str) -> String {
        format!(
            "{}/api/2.0/fs/files/{}",
            self.workspace_url,
            path.trim_start_matches('/')
        )
    }

    fn get_directories_url(&self, path: &str) -> String {
        format!(
            "{}/api/2.0/fs/directories/{}",
            self.workspace_url,
            path.trim_start_matches('/')
        )
    }

    // FilesystemHttpClient.scala:408 for get headers
    fn build_headers(
        &self,
        additional: Option<HashMap<String, String>>,
    ) -> AnyhowResult<reqwest::header::HeaderMap> {
        let mut header_map = reqwest::header::HeaderMap::new();

        // Add auth headers
        for (key, value) in &self.auth_headers {
            header_map.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes())?,
                reqwest::header::HeaderValue::from_str(value)?,
            );
        }

        // Add additional headers if provided
        if let Some(additional) = additional {
            for (key, value) in additional {
                header_map.insert(
                    reqwest::header::HeaderName::from_bytes(key.as_bytes())?,
                    reqwest::header::HeaderValue::from_str(&value)?,
                );
            }
        }

        Ok(header_map)
    }

    // Add this helper method
    fn file_info_to_object_meta(file_info: FileInfo) -> AnyhowResult<ObjectMeta> {
        let path = Path::parse(&file_info.path)?;

        let last_modified = if let Some(timestamp_ms) = file_info.last_modified {
            let timestamp_secs = timestamp_ms / 1000;
            let timestamp_nanos = (timestamp_ms % 1000) * 1_000_000;
            std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::new(timestamp_secs, timestamp_nanos as u32)
        } else {
            std::time::SystemTime::now()
        };

        Ok(ObjectMeta {
            location: path,
            last_modified: last_modified.into(),
            size: file_info.file_size.unwrap_or(0),
            e_tag: None,
            version: None,
        })
    }
}

impl fmt::Display for FilesApiHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabricksFilesObjectStore({})", self.workspace_url)
    }
}

// use presigned url if this doesnt work

// get *
// list *
// head
// put
// get_range

#[async_trait]
impl ObjectStore for FilesApiHttpClient {
    async fn get_opts(
        &self,
        location: &Path,
        _options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let path_str = location.as_ref().trim_end_matches('/');

        let content = self.get_file(path_str).await.map_err(|err| {
            let error_msg = err.to_string().to_lowercase();
            match error_msg {
                msg if msg.contains("404") => ObjectStoreError::NotFound {
                    path: location.to_string(),
                    source: err.into(),
                },
                _ => ObjectStoreError::Generic {
                    store: "FilesApiHttpClient",
                    source: err.into(),
                },
            }
        })?;

        use futures::stream;
        let stream = Box::pin(stream::once(futures::future::ready(Ok(content.clone()))));

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size: content.len() as u64,
                e_tag: None,
                version: None,
            },
            range: 0..content.len() as u64,
            attributes: Attributes::new(),
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        // Convert borrowed prefix to owned data
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let client = self.clone();

        let stream = async_stream::stream! {
            match client.list_directory(&prefix_str).await {
                Ok(directory_response) => {
                    for file_info in directory_response.contents {
                        // Only yield files, not directories
                        if !file_info.is_directory {
                        match Self::file_info_to_object_meta(file_info) {
                            Ok(meta) => yield Ok(meta),
                            Err(e) => yield Err(ObjectStoreError::Generic {
                                store: "FilesApiHttpClient",
                                source: e.into(),
                            }),
                        }
                    }
                    }
                }
                Err(e) => {
                    // Check if it's a not found error
                    if !e.to_string().to_lowercase().contains("404") {
                        yield Err(ObjectStoreError::Generic {
                            store: "FilesApiHttpClient",
                            source: e.into(),
                        });
                    }
                    // If 404, just return empty (no yield)
                }
            }
        };

        Box::pin(stream)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.list(prefix)
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let path_str = location.as_ref().trim_start_matches('/');

        self.get_head(path_str).await.map_err(|err| {
            let error_msg = err.to_string().to_lowercase();
            if error_msg.contains("404") || error_msg.contains("not found") {
                ObjectStoreError::NotFound {
                    path: location.to_string(),
                    source: err.into(),
                }
            } else {
                ObjectStoreError::Generic {
                    store: "FilesApiHttpClient",
                    source: err.into(),
                }
            }
        })
    }

    async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
        unimplemented!("we dont use this")
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        unimplemented!("we dont use this")
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        unimplemented!("we dont use this")
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        unimplemented!("we dont use this")
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        unimplemented!("we dont use this")
    }

    // You can override the provided methods if needed for optimization
    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        unimplemented!("we dont use this")
    }
}

// #[tokio::test]
// async fn test_file_http() {
//     use delta_kernel::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
//     use std::sync::Arc;

//     let files_client = FilesApiHttpClient::try_new(
//         "https://e2-dogfood.staging.cloud.databricks.com",
//         "",
//     )
//     .unwrap();

//     let object_store: Arc<dyn ObjectStore> = Arc::new(files_client);
//     let default_engine = DefaultEngine::new(object_store, Arc::new(TokioBackgroundExecutor::new()));

//     use crate::Snapshot;

//     let path = "/Volumes/jeremy-testing/test-schema/test-volume/test_table/_delta_log";
//     let url = Url::parse(&format!("file://{}", path)).unwrap();

//     let snapshot = Snapshot::try_new(url, &default_engine, None).unwrap();
//     assert_eq!(snapshot.version(), 1);
// }
