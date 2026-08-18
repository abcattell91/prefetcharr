use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post, put},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

pub struct FakeSonarr {
    state: Arc<Mutex<SonarrState>>,
    url: String,
}

struct SonarrState {
    series: Vec<Value>,
    episodes: Vec<Value>,
    tags: Vec<Value>,
    commands: Vec<Value>,
    quality_profiles: Vec<Value>,
    queue: Vec<Value>,
    next_tag_id: i32,
}

impl FakeSonarr {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(SonarrState {
            series: Vec::new(),
            episodes: Vec::new(),
            tags: Vec::new(),
            commands: Vec::new(),
            quality_profiles: Vec::new(),
            queue: Vec::new(),
            next_tag_id: 100,
        }));

        let router = Router::new()
            .route("/api", get(probe))
            .route("/api/v3/series", get(get_series))
            .route("/api/v3/series/{id}", put(put_series))
            .route("/api/v3/tag", get(get_tags).post(post_tag))
            .route("/api/v3/episode", get(get_episodes))
            .route("/api/v3/episode/monitor", put(put_episode_monitor))
            .route("/api/v3/command", post(post_command))
            .route("/api/v3/qualityprofile", get(get_quality_profiles))
            .route("/api/v3/queue", get(get_queue))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self { state, url }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn add_series(&self, series: Value) {
        self.state.lock().unwrap().series.push(series);
    }

    pub fn add_episodes(&self, episodes: Vec<Value>) {
        self.state.lock().unwrap().episodes.extend(episodes);
    }

    pub fn add_tag(&self, id: i32, label: &str) {
        self.state
            .lock()
            .unwrap()
            .tags
            .push(json!({"id": id, "label": label}));
    }

    pub fn add_quality_profile(&self, profile: Value) {
        self.state.lock().unwrap().quality_profiles.push(profile);
    }

    /// Put an episode into the download queue, as if a grab were in flight.
    pub fn enqueue(&self, series_id: i32, episode_id: i32) {
        self.state
            .lock()
            .unwrap()
            .queue
            .push(json!({"seriesId": series_id, "episodeId": episode_id}));
    }

    pub fn tags(&self) -> Vec<Value> {
        self.state.lock().unwrap().tags.clone()
    }

    // Simulate Sonarr importing (or losing) a file for an episode.
    pub fn set_has_file(&self, id: i32, has_file: bool) {
        let mut state = self.state.lock().unwrap();
        for ep in &mut state.episodes {
            if ep["id"].as_i64() == Some(i64::from(id)) {
                ep["hasFile"] = Value::Bool(has_file);
            }
        }
    }

    /// Drop everything from the download queue, as if the grabs had finished
    /// or failed.
    pub fn clear_queue(&self) {
        self.state.lock().unwrap().queue.clear();
    }

    pub fn clear_commands(&self) {
        self.state.lock().unwrap().commands.clear();
    }

    pub fn commands(&self) -> Vec<Value> {
        self.state.lock().unwrap().commands.clone()
    }

    pub fn episode(&self, id: i32) -> Value {
        self.state
            .lock()
            .unwrap()
            .episodes
            .iter()
            .find(|e| e["id"].as_i64() == Some(i64::from(id)))
            .cloned()
            .unwrap_or(Value::Null)
    }

    pub fn series_state(&self, id: i32) -> Value {
        self.state
            .lock()
            .unwrap()
            .series
            .iter()
            .find(|s| s["id"].as_i64() == Some(i64::from(id)))
            .cloned()
            .unwrap_or(Value::Null)
    }
}

pub fn make_series(id: i32, title: &str, tvdb_id: i32, seasons: &[Value]) -> Value {
    json!({
        "id": id,
        "title": title,
        "tvdbId": tvdb_id,
        "monitored": false,
        "monitorNewItems": "all",
        "qualityProfileId": 1,
        "seasons": seasons,
    })
}

pub fn make_quality_profile(id: i32, name: &str, cutoff_format_score: i32) -> Value {
    json!({
        "id": id,
        "name": name,
        "cutoff": 1,
        "cutoffFormatScore": cutoff_format_score,
        "upgradeAllowed": true,
    })
}

/// An episode backed by a file, as Sonarr reports it with
/// `includeEpisodeFile=true`.
pub fn make_episode_with_file(
    id: i32,
    series_id: i32,
    season: i32,
    episode: i32,
    quality_cutoff_not_met: bool,
    custom_format_score: i32,
) -> Value {
    let mut ep = make_episode(id, series_id, season, episode, true);
    ep["episodeFile"] = json!({
        "id": id,
        "seriesId": series_id,
        "seasonNumber": season,
        "qualityCutoffNotMet": quality_cutoff_not_met,
        "customFormatScore": custom_format_score,
    });
    ep
}

pub fn make_season(number: i32, monitored: bool, fully_aired: bool) -> Value {
    json!({
        "seasonNumber": number,
        "monitored": monitored,
        "statistics": {
            "sizeOnDisk": 9000,
            "episodeCount": 8,
            "episodeFileCount": 8,
            "totalEpisodeCount": 8,
            "nextAiring": if fully_aired { Value::Null } else { json!("2025-06-01T00:00:00Z") },
        }
    })
}

pub fn make_episode(id: i32, series_id: i32, season: i32, episode: i32, has_file: bool) -> Value {
    json!({
        "id": id,
        "seriesId": series_id,
        "seasonNumber": season,
        "episodeNumber": episode,
        "hasFile": has_file,
        "monitored": false,
        "airDateUtc": "2020-01-01T00:00:00Z",
    })
}

async fn probe() -> &'static str {
    "ok"
}

async fn get_series(State(state): State<Arc<Mutex<SonarrState>>>) -> Json<Value> {
    Json(Value::Array(state.lock().unwrap().series.clone()))
}

// Sonarr behavior: PUT /api/v3/series/{id} updates the series. When a season's
// `monitored` flips from false to true, Sonarr calls SetEpisodeMonitoredBySeason
// which sets ALL episodes in that season to monitored.
// See: NzbDrone.Core/Tv/SeriesService.cs UpdateSeries()
async fn put_series(
    State(state): State<Arc<Mutex<SonarrState>>>,
    Path(id): Path<i32>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let mut state = state.lock().unwrap();
    let id_val = i64::from(id);

    if let Some(pos) = state
        .series
        .iter()
        .position(|s| s["id"].as_i64() == Some(id_val))
    {
        let newly_monitored: Vec<i64> = body["seasons"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|new_season| new_season["monitored"].as_bool() == Some(true))
            .filter(|new_season| {
                let sn = new_season["seasonNumber"].as_i64();
                let old_seasons = state.series[pos]["seasons"].as_array();
                let was_monitored = old_seasons
                    .and_then(|ss| ss.iter().find(|s| s["seasonNumber"].as_i64() == sn))
                    .and_then(|s| s["monitored"].as_bool())
                    .unwrap_or(false);
                !was_monitored
            })
            .filter_map(|s| s["seasonNumber"].as_i64())
            .collect();

        for sn in newly_monitored {
            for ep in &mut state.episodes {
                if ep["seriesId"].as_i64() == Some(id_val)
                    && ep["seasonNumber"].as_i64() == Some(sn)
                {
                    ep["monitored"] = Value::Bool(true);
                }
            }
        }

        state.series[pos] = body;
    }

    Json(json!({}))
}

async fn get_tags(State(state): State<Arc<Mutex<SonarrState>>>) -> Json<Value> {
    Json(Value::Array(state.lock().unwrap().tags.clone()))
}

async fn post_tag(
    State(state): State<Arc<Mutex<SonarrState>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let mut state = state.lock().unwrap();
    let id = state.next_tag_id;
    state.next_tag_id += 1;
    let tag = json!({"id": id, "label": body["label"]});
    state.tags.push(tag.clone());
    Json(tag)
}

async fn get_quality_profiles(State(state): State<Arc<Mutex<SonarrState>>>) -> Json<Value> {
    Json(Value::Array(state.lock().unwrap().quality_profiles.clone()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueQuery {
    series_ids: Option<i32>,
}

async fn get_queue(
    State(state): State<Arc<Mutex<SonarrState>>>,
    Query(q): Query<QueueQuery>,
) -> Json<Value> {
    let state = state.lock().unwrap();
    let records: Vec<_> = state
        .queue
        .iter()
        .filter(|r| {
            q.series_ids
                .is_none_or(|id| r["seriesId"].as_i64() == Some(i64::from(id)))
        })
        .cloned()
        .collect();
    Json(json!({
        "page": 1,
        "pageSize": 200,
        "totalRecords": records.len(),
        "records": records,
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EpisodeQuery {
    series_id: i32,
    season_number: Option<i32>,
    include_episode_file: Option<bool>,
}

async fn get_episodes(
    State(state): State<Arc<Mutex<SonarrState>>>,
    Query(q): Query<EpisodeQuery>,
) -> Json<Value> {
    let state = state.lock().unwrap();
    let series_id = i64::from(q.series_id);
    let filtered: Vec<_> = state
        .episodes
        .iter()
        .filter(|e| e["seriesId"].as_i64() == Some(series_id))
        .filter(|e| {
            q.season_number
                .is_none_or(|sn| e["seasonNumber"].as_i64() == Some(i64::from(sn)))
        })
        .cloned()
        .map(|mut e| {
            // Sonarr only embeds the file when it is asked to.
            if q.include_episode_file != Some(true)
                && let Some(e) = e.as_object_mut()
            {
                e.remove("episodeFile");
            }
            e
        })
        .collect();
    Json(Value::Array(filtered))
}

async fn put_episode_monitor(
    State(state): State<Arc<Mutex<SonarrState>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let mut state = state.lock().unwrap();
    let ids = body["episodeIds"].as_array().unwrap();
    let monitored = body["monitored"].as_bool().unwrap();

    for ep in &mut state.episodes {
        if ids.iter().any(|id| id.as_i64() == ep["id"].as_i64()) {
            ep["monitored"] = Value::Bool(monitored);
        }
    }

    Json(json!([]))
}

// Sonarr behavior for search commands:
//
// SeasonSearch (SeasonSearchService.cs) calls ReleaseSearchService.SeasonSearch
// with monitoredOnly=true. The MonitoredEpisodeSpecification decision engine
// check will reject any release where:
//   - series.monitored is false, OR
//   - any episode in the release has monitored=false
// Season-level monitoring is NOT checked by the decision engine; it only matters
// as a propagation mechanism (flipping season.monitored monitors all episodes).
//
// EpisodeSearch (EpisodeSearchService.cs) calls ReleaseSearchService.EpisodeSearch
// with monitoredOnly=false (hardcoded). The MonitoredEpisodeSpecification skips
// its check entirely when monitoredOnly=false, so unmonitored episodes CAN be
// grabbed via EpisodeSearch.
//
// This means: before issuing a SeasonSearch, the caller must ensure the series
// and all episodes in the season are monitored. EpisodeSearch has no such
// requirement.
async fn post_command(
    State(state): State<Arc<Mutex<SonarrState>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let mut state = state.lock().unwrap();

    if body["name"].as_str() == Some("SeasonSearch") {
        let series_id = body["seriesId"].as_i64().unwrap();
        let season_number = body["seasonNumber"].as_i64().unwrap();

        let series = state
            .series
            .iter()
            .find(|s| s["id"].as_i64() == Some(series_id));

        assert!(
            series.is_some_and(|s| s["monitored"].as_bool() == Some(true)),
            "SeasonSearch requires the series to be monitored"
        );

        let unmonitored: Vec<i64> = state
            .episodes
            .iter()
            .filter(|e| {
                e["seriesId"].as_i64() == Some(series_id)
                    && e["seasonNumber"].as_i64() == Some(season_number)
            })
            .filter(|e| e["monitored"].as_bool() != Some(true))
            .filter_map(|e| e["episodeNumber"].as_i64())
            .collect();

        assert!(
            unmonitored.is_empty(),
            "SeasonSearch for season {season_number} would be rejected by Sonarr: \
             episodes {unmonitored:?} are not monitored"
        );
    }

    let now = utc_timestamp();
    match body["name"].as_str() {
        Some("EpisodeSearch") => {
            let ids: Vec<i64> = body["episodeIds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_i64)
                .collect();
            for ep in &mut state.episodes {
                if ids.contains(&ep["id"].as_i64().unwrap_or(-1)) {
                    ep["lastSearchTime"] = Value::String(now.clone());
                }
            }
        }
        Some("SeasonSearch") => {
            let series_id = body["seriesId"].as_i64();
            let season = body["seasonNumber"].as_i64();
            for ep in &mut state.episodes {
                if ep["seriesId"].as_i64() == series_id && ep["seasonNumber"].as_i64() == season {
                    ep["lastSearchTime"] = Value::String(now.clone());
                }
            }
        }
        _ => {}
    }

    state.commands.push(body);
    Json(json!({}))
}

/// Current time as Sonarr serializes it (`YYYY-MM-DDTHH:MM:SSZ`).
///
/// Lives here rather than in `util::time` because only the fake ever needs to
/// go from epoch seconds back to a string.
fn utc_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
        .cast_signed();
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));

    // Howard Hinnant's civil_from_days, the inverse of the parser's
    // days_from_civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}
