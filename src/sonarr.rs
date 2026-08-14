use std::collections::HashSet;

use anyhow::{Context, Result, anyhow};
use reqwest::{
    Url,
    header::{HeaderMap, HeaderValue},
};
use rustls_platform_verifier::ConfigVerifierExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tracing::{debug, error, info, instrument, warn};

#[derive(Debug)]
pub enum Tag {
    Label(String),
    Id(i32),
}

/// Stand-in for a tag ID that a dry run did not actually create.
const DRY_RUN_TAG_ID: i32 = -1;

#[derive(Clone)]
pub struct Client {
    base_url: Url,
    http: reqwest::Client,
    dry_run: bool,
}

impl Client {
    /// With `dry_run` set, every request that would change something in Sonarr
    /// is logged and skipped instead.
    pub fn new(base_url: &str, api_key: &str, dry_run: bool) -> Result<Self> {
        let mut api_key = HeaderValue::from_str(api_key)?;
        api_key.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert("X-Api-Key", api_key);
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .tls_backend_preconfigured(rustls::ClientConfig::with_platform_verifier()?)
            .build()?;

        let base_url = base_url.parse()?;

        Ok(Self {
            base_url,
            http,
            dry_run,
        })
    }

    fn url(&self, path: &str) -> Result<Url> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow!("url is relative"))?
            .push("api")
            .push("v3")
            .extend(path.split('/'));
        Ok(url)
    }

    /// Log a write instead of performing it. Returns `true` when the caller
    /// should skip the request.
    fn skip_write(&self, method: &str, url: &Url, body: &impl Serialize) -> bool {
        if !self.dry_run {
            return false;
        }
        let body = serde_json::to_string(body).unwrap_or_else(|e| format!("<unserializable: {e}>"));
        info!(%method, %url, %body, "dry run: skipping write");
        true
    }

    async fn get<Out: DeserializeOwned, Param: Serialize + ?Sized>(
        &self,
        path: &str,
        params: Option<&Param>,
    ) -> Result<Out> {
        let get = self.http.get(self.url(path)?);
        let get = if let Some(params) = params {
            get.query(params)
        } else {
            get
        };
        let response = get.send().await?.error_for_status()?;
        Ok(response.json::<Out>().await?)
    }

    pub async fn probe(&self) -> Result<()> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow!("url is relative"))?
            .push("api");
        self.http.get(url).send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn put_series(&self, series: &SeriesResource) -> Result<serde_json::Value> {
        let url = self.url(&format!("series/{}", series.id))?;
        if self.skip_write("PUT", &url, series) {
            return Ok(json!({}));
        }
        let response = self
            .http
            .put(url)
            .json(series)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    #[instrument(skip_all)]
    pub async fn series(&self) -> Result<Vec<SeriesResource>> {
        let series = self
            .get::<Value, ()>("series", None)
            .await?
            .as_array()
            .context("not an array")?
            .iter()
            .filter_map(|s| {
                serde_json::from_value(s.clone())
                    .inspect_err(|e| debug!(series=?s, "ignoring malformed series entry: {e}"))
                    .ok()
            })
            .collect::<Vec<SeriesResource>>();
        Ok(series)
    }

    #[instrument(skip_all)]
    async fn tags(&self) -> Result<Vec<TagResource>> {
        let tags = self
            .get::<Value, ()>("tag", None)
            .await?
            .as_array()
            .context("not an array")?
            .iter()
            .filter_map(|t| {
                serde_json::from_value(t.clone())
                    .inspect_err(|e| debug!(tags=?t, "ignoring malformed tags entry: {e}"))
                    .ok()
            })
            .collect::<Vec<_>>();
        Ok(tags)
    }

    #[instrument(skip(self))]
    pub async fn resolve_tag(&self, label: &str) -> Result<i32> {
        self.tags()
            .await
            .context("retrieving tags")?
            .into_iter()
            .find_map(|t| (t.label.as_deref() == Some(label)).then_some(t.id))
            .context("tag not known")
    }

    #[instrument(skip(self))]
    pub async fn create_tag(&self, label: &str) -> Result<i32> {
        let url = self.url("tag")?;
        let body = json!({ "label": label });
        if self.skip_write("POST", &url, &body) {
            // Sonarr assigns the real id. Hand back a placeholder so the rest
            // of the dry run still reports what it would do.
            warn!(label, "dry run: using a placeholder ID for the new tag");
            return Ok(DRY_RUN_TAG_ID);
        }
        let response = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        let tag: TagResource = response.json().await?;
        info!(id = tag.id, label, "Created tag");
        Ok(tag.id)
    }

    /// Resolve a tag by label, creating it if Sonarr doesn't know it yet.
    #[instrument(skip(self))]
    pub async fn resolve_or_create_tag(&self, label: &str) -> Result<i32> {
        match self.resolve_tag(label).await {
            Ok(id) => Ok(id),
            Err(_) => self
                .create_tag(label)
                .await
                .with_context(|| format!("creating tag {label}")),
        }
    }

    #[instrument(skip_all)]
    async fn quality_profiles(&self) -> Result<Vec<QualityProfileResource>> {
        let profiles = self
            .get::<Value, ()>("qualityprofile", None)
            .await?
            .as_array()
            .context("not an array")?
            .iter()
            .filter_map(|p| {
                serde_json::from_value(p.clone())
                    .inspect_err(|e| debug!(profile=?p, "ignoring malformed quality profile: {e}"))
                    .ok()
            })
            .collect::<Vec<_>>();
        Ok(profiles)
    }

    /// Look up a quality profile by name. Sonarr profile names are unique but
    /// not case-sensitive to the user, so match accordingly.
    #[instrument(skip(self))]
    pub async fn resolve_quality_profile(&self, name: &str) -> Result<QualityProfileResource> {
        self.quality_profiles()
            .await
            .context("retrieving quality profiles")?
            .into_iter()
            .find(|p| p.name.eq_ignore_ascii_case(name))
            .with_context(|| format!("no quality profile named {name}"))
    }

    /// Episode IDs of the series that are already being downloaded.
    ///
    /// Searching for these again would duplicate a grab that is already in
    /// flight and waste indexer requests.
    #[instrument(skip(self))]
    pub async fn queued_episode_ids(&self, series_id: i32) -> Result<HashSet<i32>> {
        let queue: QueueResource = self
            .get(
                "queue",
                Some(&[
                    ("seriesIds", series_id.to_string()),
                    ("includeEpisode", "true".to_string()),
                    ("pageSize", "200".to_string()),
                ]),
            )
            .await
            .context("error fetching the download queue")?;

        Ok(queue
            .records
            .into_iter()
            .filter_map(|r| r.episode_id)
            .collect())
    }

    #[instrument(skip(self))]
    pub async fn update_tag(&self, tag: &mut Tag) {
        let Tag::Label(label) = tag else { return };
        match self.resolve_tag(label).await {
            Ok(id) => *tag = Tag::Id(id),
            Err(err) => {
                // Not a hard error as the tag may be added to Sonarr later.
                warn!(tag=%label, "cannot resolve tag ID: {err:#}");
            }
        }
    }

    /// Like [`Self::update_tag`], but for tags this program creates itself.
    ///
    /// An unknown label is the normal state until the tag is first applied, so
    /// it is not worth warning about.
    #[instrument(skip(self))]
    pub async fn resolve_tag_quiet(&self, tag: &mut Tag) {
        let Tag::Label(label) = tag else { return };
        match self.resolve_tag(label).await {
            Ok(id) => *tag = Tag::Id(id),
            Err(err) => debug!(tag=%label, "tag not in Sonarr yet: {err:#}"),
        }
    }

    async fn set_monitored_episodes(
        &self,
        episode_ids: Vec<i32>,
        monitored: bool,
    ) -> Result<serde_json::Value> {
        let url = self.url("episode/monitor")?;

        let request = EpisodeMonitoredResource {
            episode_ids,
            monitored,
        };

        if self.skip_write("PUT", &url, &request) {
            return Ok(json!([]));
        }

        let response = self
            .http
            .put(url)
            .json(&request)
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json().await?)
    }

    pub async fn update_episode_monitoring(&self, episodes: &[EpisodeResource]) -> Result<()> {
        let monitored_ids: Vec<_> = episodes
            .iter()
            .filter_map(|e| e.monitored.then_some(e.id))
            .collect();

        let unmonitored_ids: Vec<_> = episodes
            .iter()
            .filter_map(|e| (!e.monitored).then_some(e.id))
            .collect();

        if !monitored_ids.is_empty() {
            self.set_monitored_episodes(monitored_ids, true).await?;
        }

        if !unmonitored_ids.is_empty() {
            self.set_monitored_episodes(unmonitored_ids, false).await?;
        }

        Ok(())
    }

    async fn episodes(
        &self,
        series: &SeriesResource,
        include_episode_file: bool,
    ) -> Result<Vec<EpisodeResource>> {
        self.get(
            "episode",
            Some(&[
                ("seriesId", series.id.to_string()),
                ("includeEpisodeFile", include_episode_file.to_string()),
            ]),
        )
        .await
        .context("error fetching episodes")
    }

    async fn episodes_season(
        &self,
        series: &SeriesResource,
        season: &SeasonResource,
    ) -> Result<Vec<EpisodeResource>> {
        self.get(
            "episode",
            Some(&[
                ("seriesId", series.id),
                ("seasonNumber", season.season_number),
            ]),
        )
        .await
        .context("error fetching episodes")
    }

    pub async fn episode_range(
        &self,
        series: &SeriesResource,
        season_start: i32,
        episode_start: i32,
        num: usize,
        include_episode_file: bool,
    ) -> Result<Vec<EpisodeResource>> {
        let episodes = self.episodes(series, include_episode_file).await?;
        let episodes = episode_window(season_start, episode_start, num, episodes);

        Ok(episodes)
    }

    // Make sure all newly announced episodes will be monitored.
    // https://forums.sonarr.tv/t/season-monitor-toggle-option-that-doesnt-change-the-existing-episode-state/30098/9
    pub async fn monitor_unannounced_episodes(&self, series: &mut SeriesResource) -> Result<()> {
        // Make series eligible for monitoring checks
        series.monitored = true;

        // Monitor new seasons
        series.monitor_new_items = Some(NewItemMonitorTypes::All);

        // Monitor new episode announcements in last season
        if let Some(last_season) = series.seasons.last_mut() {
            last_season.monitored = true;
        }

        if let Some(last_season) = series.seasons.last() {
            // Apply monitoring but restore episode state
            let original_episodes = self.episodes_season(series, last_season).await?;
            self.put_series(series).await?;
            self.update_episode_monitoring(&original_episodes).await?;
        } else {
            // Apply monitoring
            self.put_series(series).await?;
        }

        Ok(())
    }

    pub async fn search_episodes(&self, episodes: &[EpisodeResource]) -> Result<serde_json::Value> {
        let episode_ids: Vec<_> = episodes.iter().map(|e| e.id).collect();
        // Log which episodes these are, not just their Sonarr IDs. The IDs are
        // opaque, and the one thing worth being able to check at a glance is
        // that the episode being streamed is not among them.
        let episodes_searched: Vec<String> = episodes
            .iter()
            .map(|e| format!("s{:02}e{:02}", e.season_number, e.episode_number))
            .collect();
        info!(?episode_ids, ?episodes_searched, "Searching episodes");
        let cmd = json!({
            "name": "EpisodeSearch",
            "episodeIds": episode_ids,
        });

        self.command(cmd).await
    }

    pub async fn search_season(
        &self,
        series: &mut SeriesResource,
        season_num: i32,
    ) -> Result<serde_json::Value> {
        info!(num = season_num, "Searching season");

        let season = series
            .season(season_num)
            .with_context(|| format!("there is no season {season_num}"))?;

        if season.monitored {
            let mut season_episodes = self.episodes_season(series, season).await?;
            for e in &mut season_episodes {
                e.monitored = true;
            }
            self.update_episode_monitoring(&season_episodes).await?;
        }

        if !season.monitored {
            let season = series
                .season_mut(season_num)
                .with_context(|| format!("there is no season {season_num}"))?;
            season.monitored = true;
            self.put_series(series).await?;
        }

        let cmd = json!({
            "name": "SeasonSearch",
            "seriesId": series.id,
            "seasonNumber": season_num,
        });

        self.command(cmd).await
    }

    async fn command(&self, cmd: Value) -> std::result::Result<Value, anyhow::Error> {
        let url = self.url("command")?;

        if self.skip_write("POST", &url, &cmd) {
            return Ok(json!({}));
        }

        let response = self
            .http
            .post(url)
            .json(&cmd)
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json().await?)
    }
}

impl From<String> for Tag {
    fn from(label: String) -> Self {
        Tag::Label(label)
    }
}

fn episode_window(
    season_start: i32,
    episode_start: i32,
    num: usize,
    mut episodes: Vec<EpisodeResource>,
) -> Vec<EpisodeResource> {
    episodes.sort_by_key(|e| (e.season_number, e.episode_number));

    episodes
        .into_iter()
        .skip_while(|ep| ep.season_number != season_start || ep.episode_number != episode_start)
        .skip(1)
        .scan((season_start, episode_start), |(prev_s, prev_ep), ep| {
            let season_delta = ep.season_number.saturating_sub(*prev_s);
            let episode_delta = ep.episode_number.saturating_sub(*prev_ep);
            *prev_s = ep.season_number;
            *prev_ep = ep.episode_number;

            // filter gaps
            if (season_delta == 1 && ep.episode_number == 1)
                || (season_delta == 0 && episode_delta == 1)
            {
                Some(ep)
            } else if season_delta == 0 && episode_delta == 0 {
                warn!(?ep, "duplicated episode listing");
                Some(ep)
            } else {
                error!(?ep, "gap in the episode listing");
                None
            }
        })
        .take(num)
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeResource {
    pub id: i32,
    pub season_number: i32,
    pub episode_number: i32,
    pub has_file: bool,
    pub monitored: bool,
    /// Only present when requested with `includeEpisodeFile`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_file: Option<EpisodeFileResource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub air_date_utc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_search_time: Option<String>,
    #[serde(flatten)]
    other: serde_json::Value,
}

/// The file backing an episode.
///
/// `quality_cutoff_not_met` lives here rather than on the episode, which is
/// why the episode listing has to be requested with `includeEpisodeFile`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeFileResource {
    #[serde(default)]
    pub quality_cutoff_not_met: bool,
    #[serde(default)]
    pub custom_format_score: Option<i32>,
    #[serde(flatten)]
    other: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityProfileResource {
    pub id: i32,
    pub name: String,
    /// Target for "Upgrade Until Custom Format Score", independent of the
    /// quality cutoff
    #[serde(default)]
    pub cutoff_format_score: Option<i32>,
    #[serde(default)]
    pub upgrade_allowed: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueResource {
    #[serde(default)]
    records: Vec<QueueRecordResource>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueRecordResource {
    #[serde(default)]
    episode_id: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeasonStatisticsResource {
    pub size_on_disk: i64,
    pub episode_count: i32,
    pub episode_file_count: i32,
    pub total_episode_count: i32,
    pub next_airing: Option<String>,
    #[serde(flatten)]
    other: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeasonResource {
    pub season_number: i32,
    pub monitored: bool,
    pub statistics: Option<SeasonStatisticsResource>,
    #[serde(flatten)]
    other: serde_json::Value,
}

impl SeasonResource {
    pub fn is_fully_aired(&self) -> bool {
        !matches!(
            self.statistics,
            Some(SeasonStatisticsResource {
                next_airing: Some(_),
                ..
            })
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NewItemMonitorTypes {
    All,
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesResource {
    pub id: i32,
    pub title: Option<String>,
    pub tvdb_id: i32,
    pub monitored: bool,
    // Always sent by Sonarr; defaulted so a series entry that somehow omits it
    // is still usable rather than being dropped as malformed.
    #[serde(default)]
    pub quality_profile_id: i32,
    // optional for v3 compatibility
    pub monitor_new_items: Option<NewItemMonitorTypes>,
    pub seasons: Vec<SeasonResource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<i32>>,
    #[serde(flatten)]
    other: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagResource {
    pub id: i32,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeMonitoredResource {
    pub episode_ids: Vec<i32>,
    pub monitored: bool,
}

impl SeriesResource {
    pub fn season_mut(&mut self, num: i32) -> Option<&mut SeasonResource> {
        self.seasons.iter_mut().find(|s| s.season_number == num)
    }

    pub fn season(&self, num: i32) -> Option<&SeasonResource> {
        self.seasons.iter().find(|s| s.season_number == num)
    }

    pub fn is_tagged_with(&self, tag: &Tag) -> Option<bool> {
        let Tag::Id(id) = tag else { return None };
        Some(self.tags.as_ref()?.contains(id))
    }

    /// Switch the series to a quality profile and mark it as boosted.
    ///
    /// Existing tags are preserved — the boost tag is what lets the user find
    /// and mass-revert boosted series in Sonarr's series editor later.
    /// Returns whether anything actually changed.
    pub fn apply_boost(&mut self, quality_profile_id: i32, tag_id: i32) -> bool {
        let mut changed = false;

        if self.quality_profile_id != quality_profile_id {
            self.quality_profile_id = quality_profile_id;
            changed = true;
        }

        let tags = self.tags.get_or_insert_with(Vec::new);
        if !tags.contains(&tag_id) {
            tags.push(tag_id);
            changed = true;
        }

        if !self.monitored {
            self.monitored = true;
            changed = true;
        }

        changed
    }
}

#[cfg(test)]
mod test {
    use httpmock::Method::{GET, POST, PUT};
    use serde_json::{Value, json};

    use crate::sonarr::{
        EpisodeResource, NewItemMonitorTypes, SeasonResource, SeasonStatisticsResource,
        SeriesResource, Tag,
    };

    // API key is sent via X-Api-Key header on requests
    #[tokio::test]
    async fn auth() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series")
                    .header("X-Api-Key", "secret");
                then.json_body(serde_json::json!([]));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let _ = client.series().await?;

        series_mock.assert_async().await;

        Ok(())
    }

    // Parses series response without monitorNewItems field (Sonarr v3 compatibility)
    #[tokio::test]
    async fn series_v3() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series");
                then.json_body(serde_json::json!(
                    [{
                        "id": 1234,
                        "title": "TestShow",
                        "tvdbId": 5678,
                        "monitored": false,
                        "seasons": []
                    }]
                ));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let series = client.series().await?;
        assert_eq!(series[0].id, 1234);

        series_mock.assert_async().await;

        Ok(())
    }

    // Parses multiple series entries from a single API response
    #[tokio::test]
    async fn series_multiple() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series");
                then.json_body(serde_json::json!(
                    [{
                        "id": 1234,
                        "title": "TestShow",
                        "tvdbId": 5678,
                        "monitored": false,
                        "monitorNewItems": "all",
                        "seasons": []
                    },{
                        "id": 1234,
                        "title": "TestShow",
                        "tvdbId": 5678,
                        "monitored": false,
                        "monitorNewItems": "all",
                        "seasons": []
                    }]
                ));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let series = client.series().await?;
        assert_eq!(series.len(), 2);

        series_mock.assert_async().await;

        Ok(())
    }

    // Parses seasons that lack a statistics field
    #[tokio::test]
    async fn series_parse_missing_statistics() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series");
                then.json_body(serde_json::json!(
                    [{
                        "id": 1234,
                        "title": "TestShow",
                        "tvdbId": 5678,
                        "monitored": false,
                        "monitorNewItems": "all",
                        "seasons": [{
                            "seasonNumber": 0,
                            "monitored": false
                        }]
                    }]
                ));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let series = client.series().await?;
        assert_eq!(series.len(), 1);

        series_mock.assert_async().await;

        Ok(())
    }

    // Malformed series entries are silently skipped, valid ones still returned
    #[tokio::test]
    async fn series_skip_malformed_series() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series");
                then.json_body(serde_json::json!(
                    [{
                        "invalid": "TestShow",
                    },{
                        "id": 1234,
                        "title": "TestShow",
                        "tvdbId": 5678,
                        "monitored": false,
                        "monitorNewItems": "all",
                        "seasons": []
                    }]
                ));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let series = client.series().await?;
        assert_eq!(series.len(), 1);

        series_mock.assert_async().await;

        Ok(())
    }

    // Empty series list from the API returns an empty vec
    #[tokio::test]
    async fn series_emtpy() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series");
                then.json_body(serde_json::json!([]));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let series = client.series().await?;
        assert_eq!(series.len(), 0);

        series_mock.assert_async().await;

        Ok(())
    }

    // PUT request serializes series resource with correct camelCase JSON body
    #[tokio::test]
    async fn put_series() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let series = SeriesResource {
            id: 1234,
            title: Some("TestShow".to_string()),
            tvdb_id: 5678,
            monitored: false,
            quality_profile_id: 1,
            monitor_new_items: Some(NewItemMonitorTypes::All),
            seasons: vec![],
            tags: Some(vec![1]),
            other: Value::Null,
        };

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series/1234")
                    .method(PUT)
                    .json_body(serde_json::json!(
                        {
                            "id": 1234,
                            "title": "TestShow",
                            "tvdbId": 5678,
                            "monitored": false,
                            "qualityProfileId": 1,
                            "monitorNewItems": "all",
                            "seasons": [],
                            "tags": [1]
                        }
                    ));
                then.json_body(json!({}));
            })
            .await;
        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        client.put_series(&series).await?;

        series_mock.assert_async().await;

        Ok(())
    }

    // Season search monitors the season via PUT, then issues a SeasonSearch command
    #[tokio::test]
    async fn search_season() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let season = SeasonResource {
            season_number: 1,
            monitored: false,
            statistics: SeasonStatisticsResource {
                size_on_disk: 9000,
                episode_count: 8,
                episode_file_count: 8,
                total_episode_count: 0,
                next_airing: None,
                other: Value::Null,
            }
            .into(),
            other: Value::Null,
        };

        let mut series = SeriesResource {
            id: 1234,
            title: Some("TestShow".to_string()),
            tvdb_id: 5678,
            monitored: false,
            quality_profile_id: 1,
            monitor_new_items: Some(NewItemMonitorTypes::All),
            seasons: vec![season],
            tags: None,
            other: serde_json::json!({}),
        };

        let command_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/command")
                    .method(POST)
                    .json_body(json!({
                        "name": "SeasonSearch",
                        "seriesId": 1234,
                        "seasonNumber": 1,
                    }));
                then.json_body(json!({}));
            })
            .await;

        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        let series_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/series/1234")
                    .method(PUT)
                    .json_body(serde_json::json!({
                        "id": 1234,
                        "title": "TestShow",
                        "tvdbId": 5678,
                        "monitored": false,
                        "qualityProfileId": 1,
                        "monitorNewItems": "all",
                        "seasons": [{
                            "seasonNumber": 1,
                            "monitored": true,
                            "statistics": {
                                "sizeOnDisk": 9000,
                                "episodeCount": 8,
                                "episodeFileCount": 8,
                                "totalEpisodeCount": 0,
                                "nextAiring": null,
                            }
                        }]
                    }));
                then.json_body(json!({}));
            })
            .await;

        client.search_season(&mut series, 1).await?;

        series_mock.assert_async().await;
        command_mock.assert_async().await;

        Ok(())
    }

    // Already-monitored season explicitly monitors all episodes before searching
    #[tokio::test]
    async fn search_season_already_monitored() -> Result<(), Box<dyn std::error::Error>> {
        let server = httpmock::MockServer::start_async().await;

        let season = SeasonResource {
            season_number: 1,
            monitored: true,
            statistics: SeasonStatisticsResource {
                size_on_disk: 9000,
                episode_count: 2,
                episode_file_count: 0,
                total_episode_count: 2,
                next_airing: None,
                other: Value::Null,
            }
            .into(),
            other: Value::Null,
        };

        let mut series = SeriesResource {
            id: 1234,
            title: Some("TestShow".to_string()),
            tvdb_id: 5678,
            monitored: false,
            quality_profile_id: 1,
            monitor_new_items: Some(NewItemMonitorTypes::All),
            seasons: vec![season],
            tags: None,
            other: serde_json::json!({}),
        };

        let episodes_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/episode")
                    .query_param("seriesId", "1234")
                    .query_param("seasonNumber", "1")
                    .method(GET);
                then.json_body(json!([
                    {"id": 1, "seasonNumber": 1, "episodeNumber": 1, "hasFile": false, "monitored": false},
                    {"id": 2, "seasonNumber": 1, "episodeNumber": 2, "hasFile": false, "monitored": false},
                ]));
            })
            .await;

        let monitor_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/episode/monitor")
                    .method(PUT)
                    .json_body(json!({"episodeIds": [1, 2], "monitored": true}));
                then.json_body(json!([]));
            })
            .await;

        let command_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/command")
                    .method(POST)
                    .json_body(json!({
                        "name": "SeasonSearch",
                        "seriesId": 1234,
                        "seasonNumber": 1,
                    }));
                then.json_body(json!({}));
            })
            .await;

        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        client.search_season(&mut series, 1).await?;

        episodes_mock.assert_async().await;
        monitor_mock.assert_async().await;
        command_mock.assert_async().await;

        Ok(())
    }

    // Returns empty when requesting 0 episodes or when no next episode exists
    #[test]
    fn episode_window_none() {
        let episodes = vec![EpisodeResource {
            ..default_episode()
        }];

        assert!(super::episode_window(1, 1, 0, episodes.clone()).is_empty());
        assert!(super::episode_window(1, 1, 1, episodes.clone()).is_empty());
    }

    // Stops collecting episodes when there is a gap in episode or season numbering
    #[test]
    fn episode_window_gap() {
        let episodes = vec![
            EpisodeResource {
                episode_number: 1,
                ..default_episode()
            },
            EpisodeResource {
                episode_number: 3,
                ..default_episode()
            },
        ];

        assert!(super::episode_window(1, 1, 1, episodes.clone()).is_empty());

        let episodes = vec![
            EpisodeResource {
                season_number: 1,
                ..default_episode()
            },
            EpisodeResource {
                season_number: 3,
                ..default_episode()
            },
        ];

        assert!(super::episode_window(1, 1, 1, episodes.clone()).is_empty());

        let episodes = vec![
            EpisodeResource {
                episode_number: 1,
                season_number: 1,
                ..default_episode()
            },
            EpisodeResource {
                episode_number: 2,
                season_number: 2,
                ..default_episode()
            },
        ];

        assert!(super::episode_window(1, 1, 1, episodes.clone()).is_empty());
    }

    // Window continues into the next season when the current season ends
    #[test]
    fn episode_window_next_season() {
        let episodes = vec![
            EpisodeResource {
                episode_number: 8,
                season_number: 1,
                ..default_episode()
            },
            EpisodeResource {
                episode_number: 1,
                season_number: 2,
                ..default_episode()
            },
        ];

        let res = super::episode_window(1, 8, 1, episodes.clone());
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].season_number, 2);
    }

    // Returns multiple consecutive episodes spanning a season boundary
    #[test]
    fn episode_window_several() {
        let episodes = vec![
            EpisodeResource {
                episode_number: 8,
                season_number: 1,
                ..default_episode()
            },
            EpisodeResource {
                episode_number: 1,
                season_number: 2,
                ..default_episode()
            },
            EpisodeResource {
                episode_number: 2,
                season_number: 2,
                ..default_episode()
            },
        ];

        let res = super::episode_window(1, 8, 2, episodes.clone());
        assert_eq!(res.len(), 2);

        assert_eq!(res[0].episode_number, 1);
        assert_eq!(res[0].season_number, 2);

        assert_eq!(res[1].episode_number, 2);
        assert_eq!(res[1].season_number, 2);
    }

    // Duplicate episode listings are included with a warning instead of breaking the window
    #[test]
    fn episode_window_duplicate() {
        let episodes = vec![
            EpisodeResource {
                episode_number: 1,
                ..default_episode()
            },
            EpisodeResource {
                id: 2,
                episode_number: 2,
                ..default_episode()
            },
            EpisodeResource {
                id: 3,
                episode_number: 2,
                ..default_episode()
            },
            EpisodeResource {
                id: 4,
                episode_number: 3,
                ..default_episode()
            },
        ];

        let res = super::episode_window(1, 1, 3, episodes);
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].episode_number, 2);
        assert_eq!(res[1].episode_number, 2);
        assert_eq!(res[2].episode_number, 3);
    }

    // Matches series by resolved tag ID; returns None for unresolved label
    #[test]
    fn series_match_tag() {
        let series: SeriesResource = serde_json::from_value(serde_json::json!(
            {
                "id": 1234,
                "title": "TestShow",
                "tvdbId": 5678,
                "monitored": false,
                "monitorNewItems": "all",
                "seasons": [],
                "tags": [1, 2]
            }
        ))
        .unwrap();
        assert!(series.is_tagged_with(&crate::sonarr::Tag::Id(1)).unwrap());
        assert!(series.is_tagged_with(&crate::sonarr::Tag::Id(2)).unwrap());
        assert!(!series.is_tagged_with(&crate::sonarr::Tag::Id(3)).unwrap());
        assert!(
            series
                .is_tagged_with(&crate::sonarr::Tag::Label(String::from("1")))
                .is_none()
        );
    }

    // Returns None when the series has no tags field at all
    #[test]
    fn series_no_tag() {
        let series: SeriesResource = serde_json::from_value(serde_json::json!(
            {
                "id": 1234,
                "title": "TestShow",
                "tvdbId": 5678,
                "monitored": false,
                "monitorNewItems": "all",
                "seasons": []
            }
        ))
        .unwrap();
        assert!(series.is_tagged_with(&crate::sonarr::Tag::Id(1)).is_none());
        assert!(
            series
                .is_tagged_with(&crate::sonarr::Tag::Label(String::from("1")))
                .is_none()
        );
    }

    // Resolves label to tag ID, leaves unknown labels unresolved, and skips already-resolved IDs
    #[tokio::test]
    async fn update_tag() -> anyhow::Result<()> {
        let server = httpmock::MockServer::start_async().await;
        let tags_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/tag").method(GET);
                then.json_body(json!(
                    [ { "id": 1, "label": "tag1" } ]
                ));
            })
            .await;

        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        {
            let mut tag = Tag::from(String::from("tag1"));
            client.update_tag(&mut tag).await;
            assert!(matches!(tag, Tag::Id(1)));
        }

        {
            let mut tag = Tag::from(String::from("tag2"));
            client.update_tag(&mut tag).await;
            assert!(matches!(tag, Tag::Label(_)));
        }

        {
            let mut tag = Tag::Id(1);
            client.update_tag(&mut tag).await;
            assert!(matches!(tag, Tag::Id(1)));
        }

        tags_mock.assert_calls_async(2).await;

        Ok(())
    }

    // Tag entry without a label in Sonarr leaves the local label unresolved
    #[tokio::test]
    async fn update_tag_no_label() -> anyhow::Result<()> {
        let server = httpmock::MockServer::start_async().await;
        let tags_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/tag").method(GET);
                then.json_body(json!(
                    [ { "id": 1 } ]
                ));
            })
            .await;

        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        {
            let mut tag = Tag::from(String::from("tag1"));
            client.update_tag(&mut tag).await;
            assert!(matches!(tag, Tag::Label(_)));
        }

        tags_mock.assert_async().await;

        Ok(())
    }

    // Empty tag list from Sonarr leaves the label unresolved
    #[tokio::test]
    async fn update_tag_no_tags() -> anyhow::Result<()> {
        let server = httpmock::MockServer::start_async().await;
        let tags_mock = server
            .mock_async(|when, then| {
                when.path("/pathprefix/api/v3/tag").method(GET);
                then.json_body(json!([]));
            })
            .await;

        let client = super::Client::new(&server.url("/pathprefix"), "secret", false)?;

        {
            let mut tag = Tag::from(String::from("tag1"));
            client.update_tag(&mut tag).await;
            assert!(matches!(tag, Tag::Label(_)));
        }

        tags_mock.assert_async().await;

        Ok(())
    }

    fn default_episode() -> EpisodeResource {
        EpisodeResource {
            id: 1,
            season_number: 1,
            episode_number: 1,
            has_file: false,
            monitored: false,
            episode_file: None,
            air_date_utc: None,
            last_search_time: None,
            other: Value::default(),
        }
    }

    // Season with next_airing = None is considered fully aired
    #[test]
    fn fully_aired_with_next_airing_null() {
        let season = SeasonResource {
            season_number: 1,
            monitored: true,
            statistics: Some(SeasonStatisticsResource {
                size_on_disk: 1000,
                episode_count: 8,
                episode_file_count: 8,
                total_episode_count: 8,
                next_airing: None,
                other: Value::Null,
            }),
            other: Value::Null,
        };
        assert!(season.is_fully_aired());
    }

    // Season with a future next_airing date is not fully aired
    #[test]
    fn not_fully_aired_with_next_airing() {
        let season = SeasonResource {
            season_number: 1,
            monitored: true,
            statistics: Some(SeasonStatisticsResource {
                size_on_disk: 1000,
                episode_count: 8,
                episode_file_count: 8,
                total_episode_count: 8,
                next_airing: Some("2025-01-01T00:00:00Z".to_string()),
                other: Value::Null,
            }),
            other: Value::Null,
        };
        assert!(!season.is_fully_aired());
    }

    // Season without statistics is treated as fully aired
    #[test]
    fn fully_aired_with_no_statistics() {
        let season = SeasonResource {
            season_number: 1,
            monitored: true,
            statistics: None,
            other: Value::Null,
        };
        assert!(season.is_fully_aired());
    }
}
