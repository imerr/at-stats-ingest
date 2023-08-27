use std::collections::HashMap;
use std::fs::File;
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
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
#[serde(default = "Config::default")]
struct Config {
    /// This specifies which projects to track
    pub projects: Vec<String>,
    /// This specifies which summaries from multiple projects to create, summary-all is auto-created
    pub summaries: HashMap<String, Vec<String>>,
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
            summaries: HashMap::new(),
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
        for (key, projects) in &self.summaries {
            if projects.is_empty() {
                return Err(format!("summary '{key}' is empty").leak());
            }
            for project in projects {
                if !self.projects.contains(project) {
                    return Err(format!("summary '{key}' contains unknown project '{project}'. Did you typo the name?").leak());
                }
            }
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

#[derive(Default, WriteDataPoint, Clone)]
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

#[derive(Default, WriteDataPoint, Clone)]
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
        Ok(f) => match serde_json::from_reader::<std::fs::File, Config>(f) {
            Ok(mut c) => {
                c.summaries.insert("summary-all".to_string(), c.projects.clone());
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

    let worker = tokio::spawn(work(config, cancel.clone(), http_client, influx_client));

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
    match worker.await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to join worker: {e}");
        }
    }

    info!("Good bye o/")
}

type StatsPerProject = (ProjectPoint, HashMap<String, DownloaderPoint>);

fn add_project_stats(to: &mut StatsPerProject, times: &mut Vec<i64>, from: &StatsPerProject) {
    to.0.items_done += from.0.items_done;
    to.0.items_todo += from.0.items_todo;
    to.0.items_out += from.0.items_out;
    to.0.bytes += from.0.bytes;
    times.push(from.0.time);
    for (downloader, from_d_stats) in &from.1 {
        if let Some(to_d_stats) = to.1.get_mut(downloader) {
            to_d_stats.items += from_d_stats.items;
            to_d_stats.bytes += from_d_stats.bytes;
        } else {
            to.1.insert(downloader.clone(), DownloaderPoint {
                project: to.0.project.clone(),
                downloader: downloader.clone(),
                items: from_d_stats.items,
                bytes: from_d_stats.bytes,
                time: 0,
            });
        }
    }
}

type ResultsMutex = Mutex::<HashMap<String, StatsPerProject>>;

async fn work(config: Arc<Config>, cancel: CancellationToken, http_client: Arc<HttpClient>, influx_client: Arc<InfluxClient>) {
    loop {
        let start = Instant::now();
        let results = Arc::new(ResultsMutex::new(HashMap::new()));
        let mut join_set = JoinSet::new();
        for project in &config.projects {
            join_set.spawn(fetch(
                project.clone(),
                config.clone(),
                http_client.clone(),
                influx_client.clone(),
                results.clone(),
            ));
        }
        while let Some(join) = join_set.join_next().await {
            match join {
                Ok(_) => {}
                Err(err) => {
                    error!("Failed to join task: {}", err);
                }
            }
        }
        let results = results.lock().await;
        for (summary, projects) in &config.summaries {
            let mut stats = StatsPerProject::default();
            stats.0.project = summary.clone();
            let mut times = vec![];
            let mut success = true;
            for project in projects {
                // these are expected to be here
                let res = results.get(project);
                if res.is_none() {
                    info!("No result for project {project} while calculating summary {summary}, skipping");
                    success = false;
                    break;
                }
                add_project_stats(&mut stats, &mut times, res.unwrap());
            }
            if !success {
                continue;
            }

            let time = (times.iter().map(|&x| x as i128).sum::<i128>() / times.len() as i128) as i64;
            stats.0.time = time;
            let project_point = stats.0;
            let mut downloader_points: Vec<DownloaderPoint> = stats.1.values().cloned().collect();
            for dp in &mut downloader_points {
                dp.time = time;
            }
            write_stats(project_point, downloader_points, &influx_client, &config.influx_bucket).await;
            info!("Wrote stat summary for {summary}");
        }
        select! {
            _ = cancel.cancelled() => {
                return;
            }
            _ = sleep(Duration::from_secs(config.interval) - start.elapsed()) => {continue;}
        }
    }
}

async fn fetch(project: String, config: Arc<Config>, http_client: Arc<HttpClient>, influx_client: Arc<InfluxClient>, results: Arc<ResultsMutex>) {
    let start = Instant::now();
    match http_client.get(format!("https://legacy-api.arpa.li/{project}/stats.json")).send().await {
        Ok(res) => {
            let now = Utc::now().timestamp_nanos();
            if res.status().is_success() {
                match res.text().await {
                    Ok(body) => {
                        match serde_json::from_str::<Stats>(body.as_str()) {
                            Ok(stats) => {
                                process_project_stats(stats, results, now, &project, &influx_client, &config.influx_bucket).await;
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
}

async fn process_project_stats(stats: Stats, results: Arc<ResultsMutex>, now: i64, project: &String, client: &Arc<InfluxClient>, bucket: &String) {
    let project_point = ProjectPoint {
        project: project.clone(),
        items_done: stats.total_items_done as i64,
        items_todo: stats.total_items_todo as i64,
        items_out: stats.total_items_out as i64,
        bytes: stats.domain_bytes.iter().map(|f| *f.1 as i64).sum(),
        time: now,
    };
    let mut downloader_points = vec![];
    let mut downloader_points_map = HashMap::new();
    for dl in &stats.downloaders {
        let p = DownloaderPoint {
            project: project.clone(),
            downloader: dl.clone(),
            items: *stats.downloader_count.get(dl).unwrap_or(&0f64) as i64,
            bytes: *stats.downloader_bytes.get(dl).unwrap_or(&0f64) as i64,
            time: now,
        };
        downloader_points.push(p.clone());
        downloader_points_map.insert(dl.clone(), p);
    }
    {
        let mut l = results.lock().await;
        l.insert(project.clone(), (project_point.clone(), downloader_points_map));
        drop(l);
    }
    write_stats(project_point, downloader_points, client, bucket).await;
}

async fn write_stats(project_point: ProjectPoint, downloader_points: Vec<DownloaderPoint>, client: &Arc<InfluxClient>, bucket: &String) {
    match client.write(bucket, stream::iter(vec![project_point])).await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to submit ProjectPoint to influx: {e}")
        }
    }

    match client.write(bucket, stream::iter(downloader_points)).await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to submit DownloaderPoints to influx: {e}")
        }
    }
}