use std::path::PathBuf;

use serde::Deserialize;

use crate::LegacyArgs;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaServer {
    /// Media server type
    pub r#type: crate::MediaServer,
    /// Jellyfin/Emby/Plex/Tautulli baseurl
    pub url: String,
    /// Jellyfin/Emby/Tautulli API key or Plex server token
    pub api_key: String,
    /// User IDs or names to monitor episodes for (default: empty/all users)
    #[serde(default)]
    pub users: Vec<String>,
    /// Library names to monitor episodes for. (default: empty/all libraries)
    #[serde(default)]
    pub libraries: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sonarr {
    /// Instance name, only used to tell instances apart in the logs
    pub name: Option<String>,
    /// Sonarr baseurl
    pub url: String,
    /// Sonarr API key
    pub api_key: String,
    /// Exclude series by tag
    pub exclude_tag: Option<String>,
    /// Library names served by this instance. (default: empty/all libraries)
    #[serde(default)]
    pub libraries: Vec<String>,
    /// Per-user quality boost rules
    #[serde(default)]
    pub boost: Vec<Boost>,
}

/// Upgrade the quality of upcoming episodes for specific users.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boost {
    /// User IDs or names this rule applies to
    pub users: Vec<String>,
    /// Name of the quality profile to switch the series to
    pub quality_profile: String,
    /// Number of upcoming episodes to upgrade
    #[serde(default = "default_boost_prefetch_num")]
    pub prefetch_num: usize,
    /// Tag applied to every boosted series, created if missing
    #[serde(default = "default_boost_tag")]
    pub tag: String,
    /// Don't search an episode again within this many seconds
    #[serde(default = "default_search_cooldown")]
    pub search_cooldown: u64,
}

fn default_boost_prefetch_num() -> usize {
    5
}

fn default_boost_tag() -> String {
    String::from("prefetcharr-boosted")
}

fn default_search_cooldown() -> u64 {
    60 * 60 * 12
}

/// Either a single `[sonarr]` table or an array of `[[sonarr]]` tables.
///
/// The single-table form predates multi-instance support and stays valid.
pub enum SonarrConfig {
    One(Box<Sonarr>),
    Many(Vec<Sonarr>),
}

// Hand-written rather than `#[serde(untagged)]`: an untagged enum reports only
// "data did not match any variant", discarding the error that says which field
// was wrong. Dispatching on the shape keeps the real message.
impl<'de> Deserialize<'de> for SonarrConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = toml::Value::deserialize(deserializer)?;
        match value {
            toml::Value::Array(_) => Vec::<Sonarr>::deserialize(value).map(SonarrConfig::Many),
            _ => Sonarr::deserialize(value).map(|s| SonarrConfig::One(Box::new(s))),
        }
        .map_err(serde::de::Error::custom)
    }
}

impl SonarrConfig {
    pub fn instances(&self) -> &[Sonarr] {
        match self {
            SonarrConfig::One(sonarr) => std::slice::from_ref(sonarr),
            SonarrConfig::Many(sonarr) => sonarr,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for tracing::Level {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

// Independent on/off knobs, not a state machine — an enum per pair would only
// obscure what the TOML says.
#[allow(clippy::struct_excessive_bools)]
// A mistyped key is a silent no-op otherwise: `[[sonarr-anime]]` instead of a
// second `[[sonarr]]` parses fine as an unrelated key, and the instance simply
// never exists. Fail at startup instead.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub media_server: MediaServer,
    pub sonarr: SonarrConfig,
    /// Polling interval
    pub interval: u64,
    /// Logging directory
    pub log_dir: Option<PathBuf>,
    /// Log level
    pub log_level: Option<LogLevel>,
    /// Number of episodes to make available in advance
    pub prefetch_num: usize,
    /// Always request full seasons to prefer season packs
    pub request_seasons: bool,
    /// Number of retries for the initial connection probing
    pub connection_retries: usize,
    /// Append upcoming episodes to the active player queue
    #[serde(default)]
    pub append_to_queue: bool,
    /// Log every intended change to Sonarr instead of applying it
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub legacy: bool,
}

impl From<LegacyArgs> for Config {
    fn from(
        LegacyArgs {
            media_server_type,
            media_server_url,
            media_server_api_key,
            sonarr_url,
            sonarr_api_key,
            interval,
            log_dir,
            remaining_episodes,
            users,
            connection_retries,
            libraries,
        }: LegacyArgs,
    ) -> Self {
        let media_server = MediaServer {
            r#type: media_server_type,
            url: media_server_url,
            api_key: media_server_api_key,
            users,
            libraries,
        };
        let sonarr = Sonarr {
            name: None,
            url: sonarr_url,
            api_key: sonarr_api_key,
            exclude_tag: None,
            libraries: Vec::new(),
            boost: Vec::new(),
        };
        Config {
            media_server,
            sonarr: SonarrConfig::One(Box::new(sonarr)),
            interval,
            log_dir,
            log_level: None,
            prefetch_num: remaining_episodes.into(),
            request_seasons: true,
            connection_retries,
            append_to_queue: false,
            dry_run: false,
            legacy: true,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Config;

    // The single `[sonarr]` table predating multi-instance support still parses
    #[test]
    fn single_sonarr_table() {
        let config: Config = toml::from_str(
            r#"
            interval = 450
            log_dir = "/log"
            log_level = "Info"
            prefetch_num = 30
            request_seasons = true
            connection_retries = 12

            [media_server]
            type = "Tautulli"
            url = "http://192.168.1.200:8181"
            api_key = "secret"
            libraries = [ "Television" ]

            [sonarr]
            url = "http://192.168.1.200:8989"
            api_key = "secret"
            "#,
        )
        .unwrap();

        let sonarr = config.sonarr.instances();
        assert_eq!(sonarr.len(), 1);
        assert_eq!(sonarr[0].url, "http://192.168.1.200:8989");
        assert!(sonarr[0].libraries.is_empty());
        assert!(sonarr[0].boost.is_empty());
        assert!(!config.dry_run);
        assert_eq!(config.prefetch_num, 30);
    }

    // Several `[[sonarr]]` tables with per-instance libraries and boost rules
    #[test]
    fn multiple_sonarr_tables() {
        let config: Config = toml::from_str(
            r#"
            interval = 450
            prefetch_num = 30
            request_seasons = true
            connection_retries = 12
            dry_run = true

            [media_server]
            type = "Tautulli"
            url = "http://192.168.1.200:8181"
            api_key = "secret"
            libraries = [ "Television", "Anime" ]

            [[sonarr]]
            name = "tv"
            url = "http://192.168.1.200:8989"
            api_key = "secret"
            libraries = [ "Television" ]
            exclude_tag = "no_prefetch"

              [[sonarr.boost]]
              users = [ "abcattell91" ]
              quality_profile = "HQ-1080p"

            [[sonarr]]
            name = "anime"
            url = "http://192.168.1.200:8990"
            api_key = "secret"
            libraries = [ "Anime" ]

              [[sonarr.boost]]
              users = [ "abcattell91", "42" ]
              quality_profile = "HQ-Anime"
              prefetch_num = 3
              tag = "hq-boosted"
              search_cooldown = 3600
            "#,
        )
        .unwrap();

        assert!(config.dry_run);

        let sonarr = config.sonarr.instances();
        assert_eq!(sonarr.len(), 2);

        assert_eq!(sonarr[0].name.as_deref(), Some("tv"));
        assert_eq!(sonarr[0].libraries, ["Television"]);
        assert_eq!(sonarr[0].exclude_tag.as_deref(), Some("no_prefetch"));

        // Unset boost fields fall back to their defaults
        let boost = &sonarr[0].boost[0];
        assert_eq!(boost.users, ["abcattell91"]);
        assert_eq!(boost.quality_profile, "HQ-1080p");
        assert_eq!(boost.prefetch_num, 5);
        assert_eq!(boost.tag, "prefetcharr-boosted");
        assert_eq!(boost.search_cooldown, 60 * 60 * 12);

        let boost = &sonarr[1].boost[0];
        assert_eq!(boost.users, ["abcattell91", "42"]);
        assert_eq!(boost.prefetch_num, 3);
        assert_eq!(boost.tag, "hq-boosted");
        assert_eq!(boost.search_cooldown, 3600);
    }

    // A mistyped instance key must fail loudly. Written as `[[sonarr-anime]]`
    // it is a valid but unread key, so the instance would otherwise be missing
    // with nothing in the log to say so.
    #[test]
    fn unknown_key_is_rejected() {
        let result = toml::from_str::<Config>(
            r#"
            interval = 450
            prefetch_num = 30
            request_seasons = true
            connection_retries = 12

            [media_server]
            type = "Tautulli"
            url = "http://example.com"
            api_key = "secret"

            [[sonarr]]
            name = "television"
            url = "http://example.com/sonarr"
            api_key = "secret"

            [[sonarr-anime]]
            name = "anime"
            url = "http://example.com/sonarr-anime"
            api_key = "secret"
            "#,
        );

        // Config holds API keys and deliberately has no Debug impl, so match
        // rather than unwrap_err.
        let Err(err) = result else {
            panic!("the unknown `sonarr-anime` key must be rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("sonarr-anime"), "unhelpful error: {msg}");
    }

    // A typo inside a boost rule is rejected too
    #[test]
    fn unknown_boost_key_is_rejected() {
        let result = toml::from_str::<Config>(
            r#"
            interval = 450
            prefetch_num = 30
            request_seasons = true
            connection_retries = 12

            [media_server]
            type = "Tautulli"
            url = "http://example.com"
            api_key = "secret"

            [[sonarr]]
            url = "http://example.com/sonarr"
            api_key = "secret"

              [[sonarr.boost]]
              users = [ "someone" ]
              quality_profile = "HQ"
              qualityprofile = "typo"
            "#,
        );

        let Err(err) = result else {
            panic!("the unknown `qualityprofile` key must be rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("qualityprofile"), "unhelpful error: {msg}");
    }

    // An instance without boost rules keeps working alongside one with them
    #[test]
    fn mixed_instances() {
        let config: Config = toml::from_str(
            r#"
            interval = 450
            prefetch_num = 30
            request_seasons = true
            connection_retries = 12

            [media_server]
            type = "Plex"
            url = "http://example.com"
            api_key = "secret"

            [[sonarr]]
            url = "http://example.com/sonarr"
            api_key = "secret"

            [[sonarr]]
            url = "http://example.com/sonarr-anime"
            api_key = "secret"

              [[sonarr.boost]]
              users = [ "someone" ]
              quality_profile = "HQ"
            "#,
        )
        .unwrap();

        let sonarr = config.sonarr.instances();
        assert!(sonarr[0].boost.is_empty());
        assert_eq!(sonarr[1].boost.len(), 1);
    }
}
