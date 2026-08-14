#![warn(clippy::pedantic)]

use std::{
    fmt::Display,
    fs::read_to_string,
    io::{IsTerminal, stderr},
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use anyhow::Context;
use clap::{Parser, ValueEnum};
use config::Config;
use futures::{StreamExt as _, TryStreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{media_server::plex, util::once::Seen};

mod boost;
mod config;
#[cfg(test)]
mod fake_sonarr;
mod filter;
mod media_server;
mod process;
mod sonarr;
mod util;

use media_server::embyfin;

const NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(long)]
    config: PathBuf,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct LegacyArgs {
    /// Media server type
    #[arg(long, default_value = "jellyfin")]
    media_server_type: MediaServer,
    /// Jellyfin/Emby/Plex/Tautulli baseurl
    #[arg(long, value_name = "URL")]
    media_server_url: String,
    /// Jellyfin/Emby/Tautulli API key or Plex server token
    #[arg(long, value_name = "API_KEY", env = "MEDIA_SERVER_API_KEY")]
    media_server_api_key: String,
    /// Sonarr baseurl
    #[arg(long, value_name = "URL")]
    sonarr_url: String,
    /// Sonarr API key
    #[arg(long, value_name = "API_KEY", env = "SONARR_API_KEY")]
    sonarr_api_key: String,
    /// Polling interval
    #[arg(long, value_name = "SECONDS", default_value_t = 900)]
    interval: u64,
    /// Logging directory
    #[arg(long)]
    log_dir: Option<PathBuf>,
    /// The last <NUM> episodes trigger a search
    #[arg(long, value_name = "NUM", default_value_t = 2)]
    remaining_episodes: u8,
    /// User IDs or names to monitor episodes for (default: empty/all users)
    ///
    /// Each entry here is checked against the user's ID and name
    #[arg(long, value_name = "USER", value_delimiter = ',', num_args = 0..)]
    users: Vec<String>,
    /// Number of retries for the initial connection probing
    #[arg(long, value_name = "NUM", default_value_t = 0)]
    connection_retries: usize,
    /// Library names to monitor episodes for. (default: empty/all libraries)
    #[arg(long, value_name = "LIBRARY", value_delimiter = ',', num_args = 0..)]
    libraries: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, ValueEnum)]
enum MediaServer {
    Jellyfin,
    Emby,
    Plex,
    Tautulli,
}

impl Display for MediaServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            MediaServer::Jellyfin => "Jellyfin",
            MediaServer::Emby => "Emby",
            MediaServer::Plex => "Plex",
            MediaServer::Tautulli => "Tautulli",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Message {
    NowPlaying(media_server::NowPlaying),
}

fn config() -> anyhow::Result<Config> {
    if let Ok(args) = LegacyArgs::try_parse() {
        Ok(Config::from(args))
    } else {
        let args = Args::parse();
        let toml = read_to_string(args.config.as_path())
            .with_context(|| format!("reading config from {}", args.config.to_string_lossy()))?;
        let config = toml::from_str(&toml).context("parsing TOML config")?;
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config()?;

    enable_logging(config.log_dir.as_ref(), config.log_level);

    info!("{NAME} {VERSION}");

    if config.legacy {
        warn!(
            "Legacy configuration method detected. \
            Migrate to the new TOML config to avoid future problems."
        );
    }

    if let Err(e) = run(config).await {
        error!("{e:#}");
        info!("{NAME} exits due to an error");
        return Err(e.into());
    }

    Ok(())
}

async fn run(config: Config) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel(1);

    if config.dry_run {
        warn!("Dry run: changes to Sonarr are logged but not applied");
    }

    let instances = sonarr_instances(&config).await?;

    info!("Start watching {} sessions", config.media_server.r#type);
    let interval = Duration::from_secs(config.interval);
    let (client, queue): (
        Arc<dyn media_server::Client>,
        Option<Arc<dyn media_server::Queue + Send + Sync>>,
    ) = match config.media_server.r#type {
        MediaServer::Emby | MediaServer::Jellyfin => {
            let c = Arc::new(
                embyfin::Client::new(
                    &config.media_server.url,
                    &config.media_server.api_key,
                    config.media_server.r#type.try_into()?,
                )
                .context("Invalid connection parameters")?,
            );
            let queue: Option<Arc<dyn media_server::Queue + Send + Sync>> = config
                .append_to_queue
                .then(|| c.clone() as Arc<dyn media_server::Queue + Send + Sync>);
            (c as Arc<dyn media_server::Client>, queue)
        }
        MediaServer::Plex => {
            let c = Arc::new(
                plex::Client::new(&config.media_server.url, &config.media_server.api_key)
                    .context("Invalid connection parameters")?,
            );
            let queue: Option<Arc<dyn media_server::Queue + Send + Sync>> = config
                .append_to_queue
                .then(|| c.clone() as Arc<dyn media_server::Queue + Send + Sync>);
            (c as Arc<dyn media_server::Client>, queue)
        }
        MediaServer::Tautulli => {
            let c = Arc::new(
                media_server::tautulli::Client::new(
                    &config.media_server.url,
                    &config.media_server.api_key,
                )
                .context("Invalid connection parameters")?,
            );
            if config.append_to_queue {
                warn!(
                    "append_to_queue is not supported with Tautulli (read-only). \
                     The feature will be a no-op for this backend."
                );
            }
            (c as Arc<dyn media_server::Client>, None)
        }
    };

    client.probe_with_retry(config.connection_retries).await?;

    let has_pending = Arc::new(AtomicBool::new(false));
    let pending_ttl = interval.saturating_mul(2) + Duration::from_secs(60);
    let sink = PollSender::new(tx);
    let np_updates = client
        .now_playing_updates(interval, has_pending.clone())
        .inspect_err(|err| error!("Cannot fetch sessions from media server: {err}"))
        .filter_map(async |res| res.ok()) // remove errors
        .filter(filter::users(config.media_server.users.as_slice()))
        .filter(filter::libraries(config.media_server.libraries.as_slice()))
        .map(Message::NowPlaying)
        .map(Ok) // align with the error type of `PollSender`
        .forward(sink);

    let seen = Seen::default();
    let mut actor = process::Actor::new(
        rx,
        instances,
        seen,
        config.prefetch_num,
        config.request_seasons,
        queue,
        has_pending,
        pending_ttl,
    );

    let _ = tokio::join!(np_updates, actor.process(), client.run());

    Ok(())
}

/// Build and probe a client per configured Sonarr instance.
async fn sonarr_instances(config: &Config) -> anyhow::Result<Vec<process::Instance>> {
    let mut instances = Vec::new();

    if config.sonarr.instances().is_empty() {
        anyhow::bail!("no Sonarr instance configured");
    }

    for (index, sonarr) in config.sonarr.instances().iter().enumerate() {
        let name = sonarr
            .name
            .clone()
            .unwrap_or_else(|| format!("sonarr-{}", index + 1));

        let client = sonarr::Client::new(&sonarr.url, &sonarr.api_key, config.dry_run)
            .with_context(|| format!("Invalid connection parameters for Sonarr {name}"))?;

        util::retry(config.connection_retries, async || {
            client
                .probe()
                .await
                .with_context(|| format!("Probing Sonarr {name} failed"))
        })
        .await?;

        // Surface a typo in a profile name at startup rather than at the first
        // playback. Not fatal: the profile may simply not exist yet.
        for boost in &sonarr.boost {
            match client.resolve_quality_profile(&boost.quality_profile).await {
                Ok(profile) => info!(
                    instance = name,
                    profile = profile.name,
                    id = profile.id,
                    "Boost quality profile resolved"
                ),
                Err(e) => warn!(
                    instance = name,
                    profile = boost.quality_profile,
                    "Cannot resolve the boost quality profile: {e:#}"
                ),
            }
        }

        instances.push(process::Instance::new(
            name,
            client,
            sonarr.libraries.clone(),
            sonarr.exclude_tag.clone().map(sonarr::Tag::from),
            sonarr.boost.iter().cloned().map(boost::Rule::from).collect(),
        ));
    }

    Ok(instances)
}

fn enable_logging(log_dir: Option<&PathBuf>, level: Option<config::LogLevel>) {
    // SubscriberBuilder defaults to a baked-in INFO max-level cap; without
    // this override, a Targets/EnvFilter that allows DEBUG would still be
    // gated by the inner cap and DEBUG events would never reach the writer.
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(stderr().is_terminal())
        .with_writer(stderr)
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .finish();

    let filter = if let Some(level) = level {
        tracing_subscriber::filter::Targets::new()
            .with_target(env!("CARGO_PKG_NAME"), tracing::Level::from(level))
            .boxed()
    } else {
        EnvFilter::builder().from_env_lossy().boxed()
    };

    let rolling_layer = log_dir.as_ref().map(|log_dir| {
        let file_appender = tracing_appender::rolling::daily(log_dir, "prefetcharr.log");
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(file_appender)
    });

    subscriber
        .with(filter)
        .with(rolling_layer)
        .try_init()
        .expect("setting the default subscriber");
}
