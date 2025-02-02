use chrono::{DateTime, Utc};
use logform::{Format, LogInfo};
use mongodb::{bson::doc, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use tokio::runtime::Builder;
use winston_transport::Transport;

#[derive(Debug, Serialize, Deserialize)]
struct LogDocument {
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

        let join_handle = Some(thread::spawn(move || {
            let rt = Builder::new_current_thread().build().unwrap();

            rt.block_on(async {
                let client = Client::with_uri_str(&options_clone.connection_string)
                    .await
                    .unwrap();
                let db = client.database(&options_clone.database);
                let collection = db.collection::<LogDocument>(&options_clone.collection);

                for log in receiver {
                    if let Err(e) = collection.insert_one(log).await {
                        eprintln!("Failed to write to MongoDB: {}", e);
                    }
                }
            });
        }));

        Ok(Self {
            sender,
            join_handle,
            options,
        })
    }
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

    fn get_level(&self) -> Option<&String> {
        self.options.level.as_ref()
    }

    fn get_format(&self) -> Option<&Format> {
        self.options.format.as_ref()
    }
}

impl Drop for MongoDBTransport {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            handle.join().unwrap();
        }
    }
}
