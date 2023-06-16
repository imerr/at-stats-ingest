use std::collections::HashMap;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Local, Utc};
use fern::colors::ColoredLevelConfig;
use futures::prelude::stream;
use influxdb2::Client as InfluxClient;
use influxdb2_derive::WriteDataPoint;
use log::{error, info};
use reqwest::Client as HttpClient;
use reqwest::ClientBuilder;
use serde::Deserialize;
use tokio::{select, signal};
use std::fs::File;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
#[serde(default = "Config::default")]
struct Config {
    /// This specifies which projects to track
    pub projects: Vec<String>,
    /// Interval
    pub interval: u64,
    /// Address of the influxdb service
    pub influx_host: String,
    /// Bucket for influxdb
    pub influx_bucket: String,
    /// Bucket for influxdb
    pub influx_org: String,
    /// Influx Api token
    pub influx_api_token: String,
}

impl Config {
    fn default() -> Config {
        Config {
            projects: vec![],
            interval: 60,
            influx_host: "".to_string(),
            influx_bucket: "".to_string(),
            influx_org: "".to_string(),
            influx_api_token: "".to_string(),
        }
    }

    fn validate(&self) -> Result<(), &str> {
        if self.projects.is_empty() {
            return Err("projects can't be empty");
        }
        if self.interval < 5 {
            return Err("interval lower than 5s is very impolite.");
        }
        if self.influx_host.is_empty() {
            return Err("influx_host is not set");
        }
        if self.influx_bucket.is_empty() {
            return Err("influx_bucket is not set");
        }
        if self.influx_org.is_empty() {
            return Err("influx_org is not set");
        }
        if self.influx_api_token.is_empty() {
            return Err("influx_api_token is not set");
        }
        return Ok(());
    }
}

#[derive(Deserialize)]
struct Stats {
    domain_bytes: HashMap<String, f64>,
    downloaders: Vec<String>,
    downloader_bytes: HashMap<String, f64>,
    downloader_count: HashMap<String, f64>,
    total_items_done: f64,
    total_items_todo: f64,
    total_items_out: f64,
}

#[derive(Default, WriteDataPoint)]
#[measurement = "downloader"]
struct DownloaderPoint {
    #[influxdb(tag)]
    project: String,
    #[influxdb(tag)]
    downloader: String,
    #[influxdb(field)]
    items: i64,
    #[influxdb(field)]
    bytes: i64,
    #[influxdb(timestamp)]
    time: i64,
}

#[derive(Default, WriteDataPoint)]
#[measurement = "project"]
struct ProjectPoint {
    #[influxdb(tag)]
    project: String,
    #[influxdb(field)]
    items_done: i64,
    #[influxdb(field)]
    items_todo: i64,
    #[influxdb(field)]
    items_out: i64,
    #[influxdb(field)]
    bytes: i64,
    #[influxdb(timestamp)]
    time: i64,
}


#[tokio::main]
async fn main() {
    let colors = ColoredLevelConfig::new();
    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                Local::now().format("[%Y-%m-%d_%H:%M:%S]"),
                colors.color(record.level()),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
        .ok();

    let config: Arc<Config>;
    match File::open("config.json") {
        Ok(f) => match serde_json::from_reader(f) {
            Ok(c) => {
                config = Arc::new(c);
            }
            Err(e) => {
                error!("Failed to read config from 'config.json': {}", e);
                exit(1);
            }
        },
        Err(e) => {
            error!("Failed to open 'config.json' for reading: {}", e);
            exit(1);
        }
    }

    match config.validate() {
        Ok(_) => {}
        Err(e) => {
            error!("Config failed to validate: {e}");
            exit(1);
        }
    }

    let http_client = Arc::new(ClientBuilder::new()
        .user_agent(format!("at-stats-ingest"))
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5)).build().unwrap());

    let influx_client = Arc::new(InfluxClient::new(config.influx_host.clone(), config.influx_org.clone(), config.influx_api_token.clone()));

    let cancel = CancellationToken::new();
    let mut join_set = JoinSet::new();
    for project in &config.projects {
        join_set.spawn(fetch(
            project.clone(),
            config.clone(),
            cancel.clone(),
            http_client.clone(),
            influx_client.clone(),
        ));
    }

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Ctrl+C received, shutting down..");
            cancel.cancel();
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
            exit(1);
        }
    }
    while let Some(join) = join_set.join_next().await {
        match join {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to join task: {}", err);
            }
        }
    }
    info!("Good bye o/")
}

async fn fetch(project: String, config: Arc<Config>, cancel: CancellationToken, http_client: Arc<HttpClient>, influx_client: Arc<InfluxClient>) {
    loop {
        let start = Instant::now();
        match http_client.get(format!("https://legacy-api.arpa.li/{project}/stats.json")).send().await {
            Ok(res) => {
                let now = Utc::now().timestamp_nanos();
                if res.status().is_success() {
                    match res.text().await {
                        Ok(body) => {
                            match serde_json::from_str::<Stats>(body.as_str()) {
                                Ok(stats) => {
                                    write_stats(stats, now, &project, &influx_client, &config.influx_bucket).await;
                                    info!("Fetched & wrote stats for {project} in {:.2}s", start.elapsed().as_secs_f32())
                                }
                                Err(e) => {
                                    println!("Failed to fetch {project} stats from tracker, could not parse json: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to fetch {project} stats from tracker, could not read body: {e}");
                        }
                    }
                } else {
                    error!("Failed to fetch {project} stats from tracker, got bad status: {}", res.status().as_u16());
                }
            }
            Err(e) => {
                error!("Failed to fetch {project} stats from tracker: {e}");
            }
        }
        select! {
            _ = cancel.cancelled() => {
                return;
            }
            _ = sleep(Duration::from_secs(config.interval) - start.elapsed()) => {}
        }
    }
}

async fn write_stats(stats: Stats, now: i64, project: &String, client: &Arc<InfluxClient>, bucket: &String) {
    match client.write(bucket, stream::iter(vec![ProjectPoint {
        project: project.clone(),
        items_done: stats.total_items_done as i64,
        items_todo: stats.total_items_todo as i64,
        items_out: stats.total_items_out as i64,
        bytes: stats.domain_bytes.iter().map(|f| *f.1 as i64).sum(),
        time: now,
    }])).await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to submit project {project} ProjectPoint to influx: {e}")
        }
    }
    let mut points = vec![];
    for dl in &stats.downloaders {
        points.push(DownloaderPoint {
            project: project.clone(),
            downloader: dl.clone(),
            items: *stats.downloader_count.get(dl).unwrap_or(&0f64) as i64,
            bytes: *stats.downloader_bytes.get(dl).unwrap_or(&0f64) as i64,
            time: now,
        })
    }
    match client.write(bucket, stream::iter(points)).await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to submit project {project} DownloaderPoints to influx: {e}")
        }
    }
}