use chrono::{DateTime, Utc};
use futures::StreamExt;
use logform::{Format, LogInfo};
use mongodb::{
    bson::{self, doc, Document},
    options::{FindOptions, IndexOptions},
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
};
use tokio::runtime::{Builder, Handle, Runtime};
use winston_transport::{LogQuery, Order, Transport};

#[derive(Debug, Serialize, Deserialize)]
struct LogDocument {
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    timestamp: DateTime<Utc>,
    level: String,
    message: String,
    #[serde(flatten)]
    meta: HashMap<String, serde_json::Value>,
}

pub struct MongoDBTransport {
    sender: mpsc::Sender<LogDocument>,
    join_handle: Option<thread::JoinHandle<()>>,
    options: MongoDBOptions,
    exit_signal: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct MongoDBOptions {
    pub connection_string: String,
    pub database: String,
    pub collection: String,
    pub level: Option<String>,
    pub format: Option<Format>,
}

impl MongoDBTransport {
    pub fn new(options: MongoDBOptions) -> Result<Self, mongodb::error::Error> {
        let (sender, receiver) = mpsc::channel();
        let options_clone = options.clone();
        let exit_signal = Arc::new(AtomicBool::new(false));
        let exit_signal_clone = exit_signal.clone();

        let join_handle = Some(thread::spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(async {
                let client = Client::with_uri_str(&options_clone.connection_string)
                    .await
                    .unwrap();
                let db = client.database(&options_clone.database);
                let collection = db.collection::<LogDocument>(&options_clone.collection);

                create_indexes(&collection).await.unwrap();

                while !exit_signal_clone.load(Ordering::Relaxed) {
                    match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                        Ok(log) => {
                            if let Err(e) = collection.insert_one(log).await {
                                eprintln!("Failed to write to MongoDB: {}", e);
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            });
        }));

        Ok(Self {
            sender,
            join_handle,
            options,
            exit_signal,
        })
    }

    async fn get_collection(&self) -> Collection<LogDocument> {
        let client = Client::with_uri_str(&self.options.connection_string)
            .await
            .unwrap();
        let db = client.database(&self.options.database);
        db.collection(&self.options.collection)
    }

    async fn execute_query(&self, query: &LogQuery) -> Result<Vec<LogInfo>, String> {
        let mut filter = Document::new();

        // Add timestamp range filters
        let mut timestamp_filter = Document::new();
        if let Some(from) = query.from {
            timestamp_filter.insert("$gte", from);
        }
        if let Some(until) = query.until {
            timestamp_filter.insert("$lte", until);
        }
        if !timestamp_filter.is_empty() {
            filter.insert("timestamp", timestamp_filter);
        }

        // Add level filter
        if !query.levels.is_empty() {
            filter.insert("level", doc! { "$in": &query.levels });
        }

        // Add text search if specified
        if let Some(search_term) = &query.search_term {
            filter.insert(
                "$text",
                doc! {
                    "$search": search_term
                },
            );
            // text search is faster and more optimized then regex but behaves weird eg(search_term of `log 1` matches `log` and `1`)
            // changing the search_term in LogQuery to a rust regex and converting it to mongodb regex will be more predictable and consistent
        }

        // Configure options (sort, skip, limit)
        let mut options = FindOptions::default();

        // Apply start (skip) and limit
        if let Some(start) = query.start {
            options.skip = Some(start as u64);
        }
        if let Some(limit) = query.limit {
            options.limit = Some(limit as i64);
        }

        let sort_direction = match query.order {
            Order::Ascending => 1,
            Order::Descending => -1,
        };
        options.sort = Some(doc! { "timestamp": sort_direction });

        // Apply MongoDB Projection (Query Optimization)
        if !query.fields.is_empty() {
            let mut projection = Document::new();

            // Always include "timestamp" because we use it for sorting and filtering.
            projection.insert("timestamp", 1);

            // Add user-requested fields
            for field in query.fields.iter() {
                projection.insert(field, 1);
            }

            options.projection = Some(projection);
        }

        let collection = self.get_collection().await;
        let mut cursor = collection
            .find(filter)
            .with_options(options)
            .await
            .map_err(|e| format!("Failed to execute MongoDB query: {}", e))?;

        let mut results = Vec::new();

        // Normalize requested fields
        /*let normalized_fields: Vec<String> =
        query.fields.iter().map(|f| f.to_lowercase()).collect();*/
        // because mongodb is case sensitive, i did not lowercase the fields
        let normalized_fields: std::collections::HashSet<&String> = query.fields.iter().collect();

        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => {
                    let mut log_info = document_to_loginfo(doc);

                    // Apply user-requested-field Projection (Response Filtering)
                    if !query.fields.is_empty() {
                        if !normalized_fields.contains(&"level".to_string()) {
                            log_info.level.clear();
                        }
                        if !normalized_fields.contains(&"message".to_string()) {
                            log_info.message.clear();
                        }
                        /*log_info
                        .meta
                        .retain(|k, _| normalized_fields.contains(&k.to_lowercase()));*/
                        log_info.meta.retain(|k, _| normalized_fields.contains(k));
                    }

                    results.push(log_info);
                }
                Err(e) => return Err(format!("Error reading MongoDB document: {}", e)),
            }
        }

        Ok(results)
    }
}

fn document_to_loginfo(doc: LogDocument) -> LogInfo {
    let mut meta = doc.meta;
    meta.insert(
        "timestamp".to_string(),
        serde_json::Value::from(doc.timestamp.to_rfc3339()),
    );

    LogInfo {
        level: doc.level,
        message: doc.message,
        meta,
    }
}

async fn create_indexes(collection: &Collection<LogDocument>) -> Result<(), mongodb::error::Error> {
    // Create a text index on the 'message' field
    let text_index = IndexModel::builder()
        .keys(doc! { "message": "text" })
        .options(IndexOptions::builder().background(Some(true)).build())
        .build();

    // Create a compound index for 'level' and 'timestamp'
    let compound_index = IndexModel::builder()
        .keys(doc! { "level": 1, "timestamp": 1 })
        .options(IndexOptions::builder().background(Some(true)).build())
        .build();

    // Create the indexes
    collection
        .create_indexes(vec![text_index, compound_index])
        .await?;

    Ok(())
}

impl Transport for MongoDBTransport {
    fn log(&self, info: LogInfo) {
        let doc = LogDocument {
            timestamp: Utc::now(),
            level: info.level,
            message: info.message,
            meta: info.meta,
        };

        if let Err(e) = self.sender.send(doc) {
            eprintln!("Failed to send log to the logging thread: {}", e);
        }
    }

    fn query(&self, query: &LogQuery) -> Result<Vec<LogInfo>, String> {
        tokio::task::block_in_place(|| {
            let rt_handle = match Handle::try_current() {
                Ok(handle) => {
                    // Use the existing runtime handle
                    Some(handle)
                }
                Err(_) => {
                    // If there is no current runtime, create a new one for this task
                    let new_runtime =
                        Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?;
                    let _guard = new_runtime.enter(); // Enter the new runtime
                    Some(new_runtime.handle().clone()) // Return handle of the new runtime
                }
            };

            // If we have a runtime handle (either from current or newly created), execute the query
            if let Some(handle) = rt_handle {
                // Execute the query asynchronously within a non-blocking context
                handle.block_on(async { self.execute_query(query).await })
            } else {
                Err("Failed to acquire runtime handle".to_string())
            }
        })
    }

    fn get_level(&self) -> Option<&String> {
        self.options.level.as_ref()
    }

    fn get_format(&self) -> Option<&Format> {
        self.options.format.as_ref()
    }
}

impl Drop for MongoDBTransport {
    fn drop(&mut self) {
        self.exit_signal.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join_handle.take() {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::{bson::doc, options::ClientOptions};
    use std::env;
    use tokio;

    #[tokio::test]
    async fn test_logging_persists_to_mongodb() {
        dotenv::dotenv().ok();

        let connection_string = env::var("MONGODB_URI").expect("MONGODB_URI must be set");

        let options = MongoDBOptions {
            connection_string,
            database: "winston_mongodb_test_db".to_string(),
            collection: "logs".to_string(),
            level: Some("info".to_string()),
            format: None,
        };

        let transport = MongoDBTransport::new(options.clone()).unwrap();

        let log_info = LogInfo {
            level: "info".to_string(),
            message: "Test log message".to_string(),
            meta: HashMap::new(),
        };

        transport.log(log_info);

        // Allow some time for the log to be inserted
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Verify if the log exists in the database
        let client = Client::with_options(
            ClientOptions::parse(&options.connection_string)
                .await
                .unwrap(),
        )
        .unwrap();
        let db = client.database(&options.database);
        let collection = db.collection::<LogDocument>(&options.collection);

        let filter = doc! { "message": "Test log message" };
        let result = collection.find_one(filter.clone()).await.unwrap();

        assert!(result.is_some(), "Log entry was not found in MongoDB");

        // Cleanup: Delete the test log
        collection.delete_one(filter).await.unwrap();
    }

    //#[tokio::test]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_query_logs_from_mongodb() {
        dotenv::dotenv().ok();

        let connection_string = env::var("MONGODB_URI").expect("MONGODB_URI must be set");

        let options = MongoDBOptions {
            connection_string,
            database: "winston_mongodb_test_db".to_string(),
            collection: "logs".to_string(),
            level: Some("info".to_string()),
            format: None,
        };

        let transport = MongoDBTransport::new(options.clone()).unwrap();

        // Insert multiple logs with different levels, timestamps, and messages
        let log_entries = vec![
            LogInfo {
                level: "info".to_string(),
                message: "Info log 1".to_string(),
                meta: HashMap::new(),
            },
            LogInfo {
                level: "warn".to_string(),
                message: "Warning log".to_string(),
                meta: HashMap::new(),
            },
            LogInfo {
                level: "error".to_string(),
                message: "Error log 1".to_string(),
                meta: HashMap::new(),
            },
            LogInfo {
                level: "info".to_string(),
                message: "Info log 2".to_string(),
                meta: HashMap::new(),
            },
        ];

        // Log the entries
        for log_info in log_entries {
            transport.log(log_info);
        }

        // Allow some time for the logs to be inserted and the index created(+2 secs)
        tokio::time::sleep(std::time::Duration::from_secs(2 + 2)).await;

        // Set up the query
        let query = LogQuery {
            from: Some(Utc::now() - chrono::Duration::days(1)), // Logs from the past day
            until: Some(Utc::now()),                            // Logs until now
            levels: vec!["info".to_string(), "warn".to_string()],
            //search_term: Some("log 1".to_string()), // Searching for logs with "log 1"
            search_term: Some("\"log 1\"".to_string()), // Force exact phrase match
            start: Some(0),                             // Starting from the first log
            limit: Some(5),                             // Limit the query to 5 logs
            order: Order::Descending,                   // Sort in descending order by timestamp
            fields: vec!["level".to_string(), "message".to_string()], // Return only level and message
        };

        // Perform the query
        let results = transport.query(&query).unwrap();

        // Assert that the query results match expected conditions
        assert_eq!(results.len(), 1, "Query should return 1 result");

        let levels: Vec<String> = results.iter().map(|log| log.level.clone()).collect();
        let messages: Vec<String> = results.iter().map(|log| log.message.clone()).collect();

        // Assert that the logs contain the correct levels and messages based on the query
        assert!(levels.contains(&"info".to_string()) || levels.contains(&"warn".to_string()));
        assert!(
            messages.contains(&"Info log 1".to_string())
                || messages.contains(&"Error log 1".to_string())
        );

        // Assert that only the fields specified in the query are returned
        for log in results {
            assert!(
                log.meta.is_empty(),
                "Meta data should not be included in query results"
            );
        }

        // **Cleanup: Delete all test logs added in this test**
        let collection = transport.get_collection().await;
        let cleanup_filter = doc! { "message": { "$in": ["Info log 1", "Warning log", "Error log 1", "Info log 2"] } };
        let delete_result = collection.delete_many(cleanup_filter).await.unwrap();
        println!("Deleted {} test logs", delete_result.deleted_count);
    }
}
