use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context as _, anyhow};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    Message, boost,
    media_server::{EpisodeRef, NowPlaying, PrefetchKey, Queue, Series},
    sonarr,
    util::{once::Seen, time},
};

struct Pending {
    series: Series,
    // BTreeSet so iteration is always (season, episode) ascending — the
    // backend Queue::append impls must receive episodes in playback order.
    owed: BTreeSet<EpisodeRef>,
    last_seen: Instant,
}

/// A configured Sonarr instance and the rules that apply to it.
pub struct Instance {
    pub name: String,
    pub client: sonarr::Client,
    /// Media server libraries served by this instance. Empty means any.
    pub libraries: Vec<String>,
    pub exclude_tag: Option<sonarr::Tag>,
    pub boost: Vec<boost::Rule>,
    /// The tags of `boost`, resolved lazily, to recognise series that a
    /// previous session already boosted.
    pub boost_tags: Vec<sonarr::Tag>,
}

impl Instance {
    pub fn new(
        name: String,
        client: sonarr::Client,
        libraries: Vec<String>,
        exclude_tag: Option<sonarr::Tag>,
        boost: Vec<boost::Rule>,
    ) -> Self {
        let mut labels: Vec<&str> = boost.iter().map(|r| r.tag.as_str()).collect();
        labels.sort_unstable();
        labels.dedup();
        let boost_tags = labels
            .into_iter()
            .map(|l| sonarr::Tag::from(l.to_string()))
            .collect();

        Self {
            name,
            client,
            libraries,
            exclude_tag,
            boost,
            boost_tags,
        }
    }

    fn serves(&self, library: Option<&String>) -> bool {
        self.libraries.is_empty() || library.is_some_and(|l| self.libraries.contains(l))
    }

    /// Whether this series was boosted, by this session or an earlier one.
    async fn is_boosted(&mut self, series: &sonarr::SeriesResource) -> bool {
        for tag in &mut self.boost_tags {
            // Resolved quietly: until the first series is boosted the tag
            // legitimately does not exist yet, and warning about that on every
            // poll would be crying wolf.
            self.client.resolve_tag_quiet(tag).await;
            if series.is_tagged_with(tag) == Some(true) {
                return true;
            }
        }
        false
    }
}

/// Outcome of the fetch pass, kept separate from its error so the upgrade pass
/// can still run and so the play queue learns about missing episodes either way.
struct Fetched {
    /// Episodes of the window that Sonarr has no file for
    missing: Vec<EpisodeRef>,
    /// Episode IDs already handed to an `EpisodeSearch`
    searched_ids: HashSet<i32>,
    result: anyhow::Result<()>,
}

impl Default for Fetched {
    fn default() -> Self {
        Self {
            missing: Vec::new(),
            searched_ids: HashSet::new(),
            result: Ok(()),
        }
    }
}

pub struct Actor {
    rx: mpsc::Receiver<Message>,
    instances: Vec<Instance>,
    seen: Seen<PrefetchKey>,
    prefetch_num: usize,
    request_seasons: bool,
    queue: Option<Arc<dyn Queue + Send + Sync>>,
    pending: HashMap<String, Pending>,
    has_pending: Arc<AtomicBool>,
    pending_ttl: Duration,
}

impl Actor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rx: mpsc::Receiver<Message>,
        instances: Vec<Instance>,
        seen: Seen<PrefetchKey>,
        prefetch_num: usize,
        request_seasons: bool,
        queue: Option<Arc<dyn Queue + Send + Sync>>,
        has_pending: Arc<AtomicBool>,
        pending_ttl: Duration,
    ) -> Self {
        Self {
            rx,
            instances,
            seen,
            prefetch_num,
            request_seasons,
            queue,
            pending: HashMap::new(),
            has_pending,
            pending_ttl,
        }
    }
}

impl Actor {
    pub async fn process(&mut self) {
        let mut gc = tokio::time::interval(self.pending_ttl / 4);
        gc.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(Message::NowPlaying(np)) => {
                            if let Err(e) = self.prefetch(np).await {
                                error!(err = ?e, "Failed to process");
                            }
                        }
                        None => break,
                    }
                }
                _ = gc.tick() => {
                    self.gc_pending();
                    self.publish_has_pending();
                }
            }
        }
    }

    pub async fn prefetch(&mut self, np: NowPlaying) -> anyhow::Result<()> {
        self.refresh_pending(&np);

        let result = self.run_prefetch(&np).await;

        if let Ok(Some(pairs)) = &result
            && let Some(sid) = np.session_id.clone()
            && !pairs.is_empty()
        {
            let entry = self.pending.entry(sid).or_insert_with(|| Pending {
                series: np.series.clone(),
                owed: BTreeSet::new(),
                last_seen: Instant::now(),
            });
            entry.owed.extend(pairs.iter().copied());
        }

        // Always run flush + GC, even if Sonarr work failed — pending state
        // is independent of Sonarr success.
        self.flush_queue(&np).await;
        self.gc_pending();
        self.publish_has_pending();

        result.map(|_| ())
    }

    async fn run_prefetch(&mut self, np: &NowPlaying) -> anyhow::Result<Option<Vec<EpisodeRef>>> {
        if !self.seen.once(PrefetchKey::from(np)) {
            debug!(now_playing = ?np, "skip previously processed item");
            return Ok(None);
        }

        // find the instance holding the series, and the series itself
        let (index, mut series) = self.find_series(np).await?;

        let (client, rule, already_boosted) = {
            let instance = &mut self.instances[index];
            let client = instance.client.clone();

            info!(
                instance = instance.name,
                title = series.title.clone().unwrap_or_else(|| "?".to_string()),
                now_playing = ?np
            );

            // Resolve and match exclusion tag
            if let Some(exclude_tag) = &mut instance.exclude_tag {
                client.update_tag(exclude_tag).await;
                if let Some(true) = series.is_tagged_with(exclude_tag) {
                    info!("excluded via tag");
                    return Ok(None);
                }
            }

            let rule = instance
                .boost
                .iter()
                .find(|rule| rule.matches(&np.user))
                .cloned();

            // Checked even for users without a rule: the series may carry a
            // boosted profile from someone else's session.
            let already_boosted = instance.is_boosted(&series).await;

            (client, rule, already_boosted)
        };

        // Season 0 contains specials without any particular order.
        if np.season == 0 {
            return Ok(None);
        }

        // The quality profile has to be in place before any episode listing is
        // read: `qualityCutoffNotMet` is evaluated against the series' current
        // profile, so reading first would report every existing file as good
        // enough and nothing would ever be upgraded.
        let boost = match &rule {
            Some(rule) => Some(self.apply_boost(&client, &mut series, rule).await?),
            None => None,
        };

        // A boosted series must never be searched season-wise: Sonarr replaces
        // files in place on upgrade, and a season pack covers the episode being
        // streamed right now. This holds for every viewer of the series, not
        // just the one whose rule boosted it.
        let boosted_series = boost.is_some() || already_boosted;
        if boosted_series && self.request_seasons {
            debug!("boosted series, searching episodes instead of seasons");
        }
        let request_seasons = self.request_seasons && !boosted_series;

        let searched = self
            .fetch_missing(&client, &mut series, np, request_seasons)
            .await;

        // The upgrade pass runs even when fetching failed — the two cover
        // different episodes and one shouldn't mask the other.
        let upgrade = match (&rule, &boost) {
            (Some(rule), Some(boost)) => {
                self.upgrade_existing(&client, &series, np, rule, boost, &searched.searched_ids)
                    .await
            }
            _ => Ok(()),
        };

        searched.result?;
        upgrade?;

        Ok(Some(searched.missing))
    }

    /// Switch the series onto the boosted quality profile and tag it.
    async fn apply_boost(
        &self,
        client: &sonarr::Client,
        series: &mut sonarr::SeriesResource,
        rule: &boost::Rule,
    ) -> anyhow::Result<sonarr::QualityProfileResource> {
        let profile = client
            .resolve_quality_profile(&rule.quality_profile)
            .await
            .with_context(|| format!("resolving quality profile {}", rule.quality_profile))?;

        if profile.upgrade_allowed == Some(false) {
            warn!(
                profile = profile.name,
                "quality profile has upgrades disabled, \
                 Sonarr will not replace any existing file"
            );
        }

        let tag_id = client.resolve_or_create_tag(&rule.tag).await?;

        if series.apply_boost(profile.id, tag_id) {
            info!(
                profile = profile.name,
                profile_id = profile.id,
                tag = rule.tag,
                "Boosting series quality"
            );
            client.put_series(series).await?;
        }

        Ok(profile)
    }

    /// The pre-existing behaviour: make sure the next `prefetch_num` episodes
    /// exist at all, fetching whole seasons when configured to.
    async fn fetch_missing(
        &self,
        client: &sonarr::Client,
        series: &mut sonarr::SeriesResource,
        np: &NowPlaying,
        request_seasons: bool,
    ) -> Fetched {
        let mut fetched = Fetched::default();

        let episodes = match client
            .episode_range(series, np.season, np.episode, self.prefetch_num, false)
            .await
        {
            Ok(episodes) => episodes,
            Err(e) => {
                fetched.result = Err(e);
                return fetched;
            }
        };

        if episodes.len() < self.prefetch_num {
            info!("Not as many episodes announced, monitor new items instead");
            if let Err(e) = client.monitor_unannounced_episodes(series).await {
                fetched.result = Err(e);
                return fetched;
            }
        } else if !series.monitored {
            series.monitored = true;
            if let Err(e) = client.put_series(series).await {
                fetched.result = Err(e);
                return fetched;
            }
        }

        let missing_episodes: Vec<_> = episodes.into_iter().filter(|e| !e.has_file).collect();
        fetched.missing = missing_episodes
            .iter()
            .map(|e| EpisodeRef::new(e.season_number, e.episode_number))
            .collect();
        let mut episodes_to_search = Vec::new();

        if request_seasons {
            let mut seasons_to_search: HashSet<i32> = HashSet::new();

            for e in missing_episodes {
                if series
                    .season(e.season_number)
                    .is_some_and(sonarr::SeasonResource::is_fully_aired)
                {
                    seasons_to_search.insert(e.season_number);
                } else {
                    episodes_to_search.push(e);
                }
            }

            let mut season_numbers: Vec<_> = seasons_to_search.into_iter().collect();
            season_numbers.sort_unstable();

            let mut error = false;
            for season_num in season_numbers {
                if let Err(err) = client.search_season(series, season_num).await {
                    error!("skip searching for season {season_num}: {err:#}");
                    error = true;
                }
            }
            if error {
                fetched.result = Err(anyhow!("failed searching one or more seasons"));
            }
        } else {
            episodes_to_search = missing_episodes;
        }

        if !episodes_to_search.is_empty() {
            let episodes_to_search: Vec<_> = episodes_to_search
                .into_iter()
                .map(|mut e| {
                    e.monitored = true;
                    e
                })
                .collect();
            fetched
                .searched_ids
                .extend(episodes_to_search.iter().map(|e| e.id));

            if let Err(e) = client.update_episode_monitoring(&episodes_to_search).await {
                fetched.result = Err(e);
            } else if let Err(e) = client.search_episodes(&episodes_to_search).await {
                fetched.result = Err(e);
            }
        }

        fetched
    }

    /// Search for upgrades of the next few episodes, including ones already on
    /// disk. Only ever searches individual episodes.
    async fn upgrade_existing(
        &self,
        client: &sonarr::Client,
        series: &sonarr::SeriesResource,
        np: &NowPlaying,
        rule: &boost::Rule,
        profile: &sonarr::QualityProfileResource,
        already_searched: &HashSet<i32>,
    ) -> anyhow::Result<()> {
        let episodes = client
            .episode_range(series, np.season, np.episode, rule.prefetch_num, true)
            .await
            .context("fetching episodes to upgrade")?;

        let queued = client
            .queued_episode_ids(series.id)
            .await
            .unwrap_or_else(|e| {
                // Not fatal: the worst case is a duplicate grab, which Sonarr
                // rejects on its own.
                warn!("cannot read the download queue: {e:#}");
                HashSet::new()
            });

        let now = time::now().and_then(|n| i64::try_from(n).ok()).unwrap_or(0);
        let candidates =
            boost::select_upgrades(&episodes, profile, &queued, now, rule.search_cooldown);

        let to_search: Vec<_> = candidates
            .into_iter()
            .filter(|e| !already_searched.contains(&e.id))
            .map(|mut e| {
                // A search only grabs for a monitored episode, and in a
                // prefetcharr library plenty of upcoming episodes are not.
                e.monitored = true;
                e
            })
            .collect();

        if to_search.is_empty() {
            info!("Nothing to upgrade");
            return Ok(());
        }

        client.update_episode_monitoring(&to_search).await?;
        client.search_episodes(&to_search).await?;

        Ok(())
    }

    fn refresh_pending(&mut self, np: &NowPlaying) {
        let Some(sid) = np.session_id.as_deref() else {
            return;
        };
        if let Some(p) = self.pending.get_mut(sid) {
            // If the user switched to a different series within the same
            // session, drop stale owed pairs — they belong to the previous
            // show and would never be resolvable in the new context.
            if p.series != np.series {
                p.series = np.series.clone();
                p.owed.clear();
            }
            p.last_seen = Instant::now();
        }
    }

    async fn flush_queue(&mut self, np: &NowPlaying) {
        let Some(queue) = self.queue.clone() else {
            return;
        };
        let Some(sid) = np.session_id.as_deref() else {
            return;
        };
        let Some(pending) = self.pending.get(sid) else {
            return;
        };
        if pending.owed.is_empty() {
            return;
        }
        let owed: Vec<EpisodeRef> = pending.owed.iter().copied().collect();
        match queue.append(np, &owed).await {
            Ok(success) => {
                if let Some(entry) = self.pending.get_mut(sid) {
                    entry.owed.retain(|p| !success.contains(p));
                    if entry.owed.is_empty() {
                        self.pending.remove(sid);
                    }
                }
            }
            Err(e) => {
                warn!(err = ?e, session_id = sid, "queue append failed");
            }
        }
    }

    fn gc_pending(&mut self) {
        let now = Instant::now();
        let ttl = self.pending_ttl;
        self.pending
            .retain(|_, p| now.saturating_duration_since(p.last_seen) <= ttl);
    }

    fn publish_has_pending(&self) {
        self.has_pending
            .store(!self.pending.is_empty(), Ordering::Relaxed);
    }

    /// Locate the series among the configured Sonarr instances.
    ///
    /// Only instances serving the session's library are considered; among
    /// those, the first one that knows the series wins. A series present on
    /// several instances is reported so the ambiguity can be resolved with
    /// per-instance `libraries`.
    async fn find_series(
        &mut self,
        np: &NowPlaying,
    ) -> Result<(usize, sonarr::SeriesResource), anyhow::Error> {
        let mut found: Option<(usize, sonarr::SeriesResource)> = None;

        for (index, instance) in self.instances.iter().enumerate() {
            if !instance.serves(np.library.as_ref()) {
                debug!(
                    instance = instance.name,
                    library = ?np.library,
                    "instance does not serve this library"
                );
                continue;
            }

            let series = match instance.client.series().await {
                Ok(series) => series,
                Err(e) => {
                    // One unreachable instance must not hide a series held by
                    // another.
                    error!(instance = instance.name, "cannot list series: {e:#}");
                    continue;
                }
            };

            let Some(series) = series.into_iter().find(|s| match &np.series {
                Series::Title(t) => s.title.as_ref() == Some(t),
                Series::Tvdb(i) => &s.tvdb_id == i,
            }) else {
                continue;
            };

            match &found {
                None => found = Some((index, series)),
                Some((first, _)) => warn!(
                    instance = instance.name,
                    chosen = self.instances[*first].name,
                    "series exists on more than one instance, using the first match"
                ),
            }
        }

        found.ok_or_else(|| {
            // Say which instances were actually consulted. The usual cause is
            // a library that routes to no instance, or a series held by an
            // instance the configuration never mentions.
            let considered: Vec<&str> = self
                .instances
                .iter()
                .filter(|i| i.serves(np.library.as_ref()))
                .map(|i| i.name.as_str())
                .collect();
            let configured: Vec<&str> = self.instances.iter().map(|i| i.name.as_str()).collect();
            anyhow!(
                "series {:?} (library {:?}, user {:?}) not found in Sonarr; \
                 searched instances {considered:?} out of {configured:?}",
                np.series,
                np.library,
                np.user.name,
            )
        })
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering},
        time::Duration,
    };

    use futures::{FutureExt, future::BoxFuture};
    use serde_json::json;
    use tokio::sync::mpsc;

    use crate::{
        boost,
        fake_sonarr::{
            FakeSonarr, make_episode, make_episode_with_file, make_quality_profile, make_season,
            make_series,
        },
        media_server::{EpisodeRef, NowPlaying, Queue, Series, User, test::np_default},
        util::once,
    };

    type AppendCall = (String, Vec<EpisodeRef>);

    #[derive(Default)]
    struct FakeQueue {
        calls: Mutex<Vec<AppendCall>>,
        next_success: Mutex<Option<HashSet<EpisodeRef>>>,
    }

    impl FakeQueue {
        fn calls(&self) -> Vec<AppendCall> {
            self.calls.lock().unwrap().clone()
        }

        fn override_next(&self, success: HashSet<EpisodeRef>) {
            *self.next_success.lock().unwrap() = Some(success);
        }
    }

    impl Queue for FakeQueue {
        fn append(
            &self,
            np: &NowPlaying,
            episodes: &[EpisodeRef],
        ) -> BoxFuture<'_, anyhow::Result<HashSet<EpisodeRef>>> {
            let sid = np.session_id.clone().unwrap_or_default();
            let pairs: Vec<EpisodeRef> = episodes.to_vec();
            self.calls.lock().unwrap().push((sid, pairs.clone()));
            let success = self
                .next_success
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| pairs.into_iter().collect());
            async move { Ok(success) }.boxed()
        }
    }

    /// A single Sonarr instance serving every library, with no rules attached.
    fn instance(fake: &FakeSonarr) -> super::Instance {
        instance_with(fake, Vec::new(), None, Vec::new())
    }

    fn instance_with(
        fake: &FakeSonarr,
        libraries: Vec<&str>,
        exclude_tag: Option<String>,
        boost: Vec<boost::Rule>,
    ) -> super::Instance {
        super::Instance::new(
            "test".to_string(),
            crate::sonarr::Client::new(fake.url(), "secret", false).unwrap(),
            libraries.into_iter().map(ToString::to_string).collect(),
            exclude_tag.map(crate::sonarr::Tag::from),
            boost,
        )
    }

    fn boost_rule(users: &[&str], quality_profile: &str, prefetch_num: usize) -> boost::Rule {
        boost::Rule::from(crate::config::Boost {
            users: users.iter().map(ToString::to_string).collect(),
            quality_profile: quality_profile.to_string(),
            prefetch_num,
            tag: "hq-boosted".to_string(),
            search_cooldown: 0,
        })
    }

    fn actor_with_instances(
        instances: Vec<super::Instance>,
        prefetch_num: usize,
        request_seasons: bool,
    ) -> super::Actor {
        let (_tx, rx) = mpsc::channel(1);
        super::Actor::new(
            rx,
            instances,
            once::Seen::default(),
            prefetch_num,
            request_seasons,
            None,
            Arc::new(AtomicBool::new(false)),
            Duration::from_secs(3600),
        )
    }

    fn actor_with_queue(
        fake: &FakeSonarr,
        prefetch_num: usize,
        queue: Arc<FakeQueue>,
        has_pending: Arc<AtomicBool>,
        pending_ttl: Duration,
    ) -> super::Actor {
        let (_tx, rx) = mpsc::channel(1);
        super::Actor::new(
            rx,
            vec![instance(fake)],
            once::Seen::default(),
            prefetch_num,
            false,
            Some(queue as Arc<dyn Queue + Send + Sync>),
            has_pending,
            pending_ttl,
        )
    }

    fn default_series() -> serde_json::Value {
        make_series(
            1234,
            "TestShow",
            5678,
            &[
                make_season(0, false, true),
                make_season(1, false, true),
                make_season(2, false, true),
            ],
        )
    }

    fn default_episodes() -> Vec<serde_json::Value> {
        let mut eps = Vec::new();
        for s in 1..=2 {
            for e in 1..=8 {
                eps.push(make_episode(s * 10 + e, 1234, s, e, false));
            }
        }
        eps
    }

    fn actor(fake: &FakeSonarr, prefetch_num: usize, request_seasons: bool) -> super::Actor {
        actor_with_instances(vec![instance(fake)], prefetch_num, request_seasons)
    }

    fn actor_with_tag(
        fake: &FakeSonarr,
        prefetch_num: usize,
        request_seasons: bool,
        exclude_tag: Option<String>,
    ) -> super::Actor {
        let instance = instance_with(fake, Vec::new(), exclude_tag, Vec::new());
        actor_with_instances(vec![instance], prefetch_num, request_seasons)
    }

    /// An actor whose only instance boosts `users` onto `quality_profile`.
    fn actor_with_boost(
        fake: &FakeSonarr,
        prefetch_num: usize,
        request_seasons: bool,
        rule: boost::Rule,
    ) -> super::Actor {
        let instance = instance_with(fake, Vec::new(), None, vec![rule]);
        actor_with_instances(vec![instance], prefetch_num, request_seasons)
    }

    // Prefetching from mid-season triggers season searches for the current and next season
    #[tokio::test]
    #[test_log::test]
    async fn search_next() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(
            fake.series_state(1234)["seasons"][1]["monitored"]
                .as_bool()
                .unwrap()
        );
        assert!(
            fake.series_state(1234)["seasons"][2]["monitored"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            fake.commands(),
            vec![
                json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 1}),
                json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 2}),
            ]
        );
        Ok(())
    }

    // With request_seasons disabled, prefetches individual episodes across season boundaries
    #[tokio::test]
    #[test_log::test]
    async fn search_episodes() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 3, false)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(fake.episode(18)["monitored"].as_bool().unwrap());
        assert!(fake.episode(21)["monitored"].as_bool().unwrap());
        assert!(fake.episode(22)["monitored"].as_bool().unwrap());
        assert!(!fake.episode(11)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [18, 21, 22]}),]
        );
        Ok(())
    }

    // When fewer episodes remain than prefetch_num, monitors unannounced episodes and searches only what's available
    #[tokio::test]
    #[test_log::test]
    async fn search_episodes_exceeding() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 3, false)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 2,
                ..np_default()
            })
            .await?;

        // monitor_unannounced_episodes: series monitored, last season monitored
        assert!(fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(
            fake.series_state(1234)["seasons"][2]["monitored"]
                .as_bool()
                .unwrap()
        );
        // Episode monitoring restored: all s2 episodes back to unmonitored except the searched one
        assert!(!fake.episode(21)["monitored"].as_bool().unwrap());
        assert!(fake.episode(28)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [28]}),]
        );
        Ok(())
    }

    // Watching the pilot triggers unannounced episode monitoring since remaining episodes < prefetch_num
    #[tokio::test]
    #[test_log::test]
    async fn pilot() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 1,
                season: 1,
                ..np_default()
            })
            .await?;

        // monitor_unannounced_episodes called (only 1 ep remaining in s1 < prefetch_num=2)
        assert!(fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(
            fake.series_state(1234)["seasons"][1]["monitored"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 1}),]
        );
        Ok(())
    }

    // Season 0 (specials) is skipped without triggering any searches or monitoring changes
    #[tokio::test]
    #[test_log::test]
    async fn special_episode() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 1,
                season: 0,
                ..np_default()
            })
            .await?;

        assert!(!fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // When a season is already monitored, its episodes are explicitly monitored before searching
    #[tokio::test]
    #[test_log::test]
    async fn monitor_episodes_of_monitored_season() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["seasons"][1]["monitored"] = true.into();
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 1,
                season: 1,
                ..np_default()
            })
            .await?;

        // Season 1 was already monitored → episodes must be explicitly monitored
        for e in 11..=18 {
            assert!(
                fake.episode(e)["monitored"].as_bool().unwrap(),
                "s1e{} should be monitored",
                e - 10
            );
        }
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 1}),]
        );
        Ok(())
    }

    // Seasons still airing fall back to episode search instead of season search
    #[tokio::test]
    #[test_log::test]
    async fn search_season_not_fully_aired() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["monitored"] = true.into();
        series["seasons"][1] = make_season(1, false, false);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor(&fake, 1, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(fake.episode(18)["monitored"].as_bool().unwrap());
        assert!(!fake.episode(17)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [18]}),]
        );
        Ok(())
    }

    // Fully aired seasons use season search while still-airing seasons fall back to episode search
    #[tokio::test]
    #[test_log::test]
    async fn search_season_mixed() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["monitored"] = true.into();
        series["seasons"][1] = make_season(1, false, false);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        // Season 2 fully aired → SeasonSearch
        assert!(
            fake.series_state(1234)["seasons"][2]["monitored"]
                .as_bool()
                .unwrap()
        );
        // Season 1 still airing → EpisodeSearch for s1e8
        assert!(fake.episode(18)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![
                json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 2}),
                json!({"name": "EpisodeSearch", "episodeIds": [18]}),
            ]
        );
        Ok(())
    }

    // Series with a matching exclusion tag is skipped without any searches
    #[tokio::test]
    #[test_log::test]
    async fn exclude_tag_skips_series() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_tag(1, "no-prefetch");
        let mut series = default_series();
        series["tags"] = json!([1]);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor_with_tag(&fake, 2, true, Some("no-prefetch".to_string()))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // Series without the exclusion tag is processed normally
    #[tokio::test]
    #[test_log::test]
    async fn exclude_tag_allows_untagged_series() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_tag(1, "no-prefetch");
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor_with_tag(&fake, 1, true, Some("no-prefetch".to_string()))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // Unresolved exclusion tag label does not exclude any series
    #[tokio::test]
    #[test_log::test]
    async fn exclude_tag_unresolved() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["tags"] = json!([1]);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor_with_tag(&fake, 1, true, Some("no-prefetch".to_string()))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // Duplicate NowPlaying for s01e07 is skipped on second prefetch call
    #[tokio::test]
    #[test_log::test]
    async fn deduplicate() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let np = NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 7,
            season: 1,
            ..np_default()
        };

        let mut actor = actor(&fake, 1, true);
        actor.prefetch(np.clone()).await?;
        actor.prefetch(np).await?;

        assert_eq!(
            fake.commands(),
            vec![json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 1})],
        );
        Ok(())
    }

    // Series can be found by TVDB ID instead of title
    #[tokio::test]
    #[test_log::test]
    async fn find_series_by_tvdb_id() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 1, true)
            .prefetch(NowPlaying {
                series: Series::Tvdb(5678),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // Prefetching a series not in Sonarr returns an error
    #[tokio::test]
    #[test_log::test]
    async fn series_not_found() {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());

        let result = actor(&fake, 1, true)
            .prefetch(NowPlaying {
                series: Series::Title("Unknown".to_string()),
                episode: 1,
                season: 1,
                ..np_default()
            })
            .await;

        assert!(result.is_err());
    }

    // s01e08 already has a file so only s02e01 is searched
    #[tokio::test]
    #[test_log::test]
    async fn skip_downloaded_episodes() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        let mut eps = default_episodes();
        // Mark s01e08 as already downloaded
        eps[7]["hasFile"] = true.into();
        fake.add_episodes(eps);

        actor(&fake, 2, false)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        // s01e08 has_file=true so only s02e01 is searched
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [21]})]
        );
        Ok(())
    }

    // Queue::append is invoked with the prefetch range when a session_id is
    // present and the actor has a queue handle
    #[tokio::test]
    #[test_log::test]
    async fn queue_append_called() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_secs(3600),
        );

        actor
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                session_id: Some("s1".into()),
                ..np_default()
            })
            .await?;

        let calls = queue.calls();
        assert_eq!(calls.len(), 1);
        let (sid, pairs) = &calls[0];
        assert_eq!(sid, "s1");
        // Episodes must reach the backend in playback order, even across the
        // season boundary — the player would otherwise watch them out of
        // sequence.
        assert_eq!(pairs, &vec![EpisodeRef::new(1, 8), EpisodeRef::new(2, 1)]);
        // Default fake returns full success → has_pending falls back to false
        assert!(!has_pending.load(Ordering::Relaxed));
        Ok(())
    }

    // Episodes already on disk must not be appended to the play queue —
    // some clients (e.g. Emby) play a single item and resolve "next up"
    // locally, so re-enqueueing existing files fights with the client.
    #[tokio::test]
    #[test_log::test]
    async fn queue_skips_episodes_with_file() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        let mut eps = default_episodes();
        // s01e08 is already in the library
        eps[7]["hasFile"] = true.into();
        fake.add_episodes(eps);

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_secs(3600),
        );

        actor
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                session_id: Some("s1".into()),
                ..np_default()
            })
            .await?;

        let calls = queue.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "s1");
        assert_eq!(calls[0].1, vec![EpisodeRef::new(2, 1)]);
        Ok(())
    }

    // Partial success keeps remaining pairs owed; subsequent prefetch calls
    // for the same session retry only the still-owed subset.
    #[tokio::test]
    #[test_log::test]
    async fn queue_partial_success_retries() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_secs(3600),
        );

        // First call: fake returns only s01e08 as success.
        queue.override_next([EpisodeRef::new(1, 8)].into_iter().collect());
        let np = NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 7,
            season: 1,
            session_id: Some("s1".into()),
            ..np_default()
        };
        actor.prefetch(np.clone()).await?;
        assert!(has_pending.load(Ordering::Relaxed));

        // Second call: fake returns full success — only (2, 1) is retried.
        actor.prefetch(np).await?;
        let calls = queue.calls();
        assert_eq!(calls.len(), 2);
        let second_pairs: HashSet<EpisodeRef> = calls[1].1.iter().copied().collect();
        assert_eq!(second_pairs, [EpisodeRef::new(2, 1)].into_iter().collect());
        assert!(!has_pending.load(Ordering::Relaxed));
        Ok(())
    }

    // Stale pending entries are evicted by GC after the TTL elapses, and
    // has_pending flips back to false.
    #[tokio::test]
    #[test_log::test]
    async fn queue_pending_gc() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_millis(50),
        );

        queue.override_next(HashSet::new());
        actor
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                session_id: Some("s1".into()),
                ..np_default()
            })
            .await?;
        assert!(has_pending.load(Ordering::Relaxed));

        // Wait past TTL, then drive GC by processing a different unrelated event.
        tokio::time::sleep(Duration::from_millis(80)).await;
        // process something that doesn't add to pending — just any np without
        // session_id so the gc still runs.
        actor
            .prefetch(NowPlaying {
                series: Series::Tvdb(99999),
                episode: 1,
                season: 1,
                session_id: None,
                ..np_default()
            })
            .await
            .ok();

        assert!(!has_pending.load(Ordering::Relaxed));
        Ok(())
    }

    // No queue handle → no append calls; pending stays empty
    #[tokio::test]
    #[test_log::test]
    async fn queue_no_op_without_handle() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        // Default actor has no queue handle
        let mut a = actor(&fake, 2, false);
        a.prefetch(NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 7,
            season: 1,
            session_id: Some("s1".into()),
            ..np_default()
        })
        .await?;

        // No has_pending observable here, but no panic either.
        // The Sonarr search still happened, and that's covered by other tests.
        Ok(())
    }

    fn boosted_user() -> User {
        User {
            name: "Boosted".to_string(),
            id: "42".to_string(),
        }
    }

    /// A library where every episode is on disk but below the boosted
    /// profile's cutoff.
    fn low_quality_episodes() -> Vec<serde_json::Value> {
        let mut eps = Vec::new();
        for s in 1..=2 {
            for e in 1..=8 {
                eps.push(make_episode_with_file(s * 10 + e, 1234, s, e, true, 0));
            }
        }
        eps
    }

    /// Same library, but every file already satisfies the profile.
    fn high_quality_episodes() -> Vec<serde_json::Value> {
        let mut eps = Vec::new();
        for s in 1..=2 {
            for e in 1..=8 {
                eps.push(make_episode_with_file(s * 10 + e, 1234, s, e, false, 1000));
            }
        }
        eps
    }

    fn boosted_np() -> NowPlaying {
        NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 5,
            season: 1,
            user: boosted_user(),
            ..np_default()
        }
    }

    // A boosted user on a low-quality series gets the profile switched, the tag
    // applied, the next N episodes monitored and one EpisodeSearch
    #[tokio::test]
    #[test_log::test]
    async fn boost_upgrades_existing_files() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(low_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(boosted_np())
            .await?;

        let series = fake.series_state(1234);
        assert_eq!(series["qualityProfileId"].as_i64(), Some(9));
        // The tag did not exist and must have been created
        assert_eq!(fake.tags().len(), 1);
        let tag_id = fake.tags()[0]["id"].as_i64().unwrap();
        assert_eq!(fake.tags()[0]["label"].as_str(), Some("hq-boosted"));
        assert_eq!(series["tags"], json!([tag_id]));

        // s01e06 and s01e07 are the next two, both already on disk but below
        // cutoff, so they are monitored and searched individually.
        assert!(fake.episode(16)["monitored"].as_bool().unwrap());
        assert!(fake.episode(17)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [16, 17]})]
        );
        Ok(())
    }

    // Episodes already at the profile's cutoff produce no monitor call and no
    // search command
    #[tokio::test]
    #[test_log::test]
    async fn boost_nothing_to_upgrade() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(high_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(boosted_np())
            .await?;

        // The profile was still applied — that is what makes future upgrades
        // possible — but nothing needed searching.
        assert_eq!(
            fake.series_state(1234)["qualityProfileId"].as_i64(),
            Some(9)
        );
        assert!(fake.commands().is_empty());
        assert!(!fake.episode(16)["monitored"].as_bool().unwrap());
        Ok(())
    }

    // The episode being streamed is never monitored or searched — Sonarr
    // replaces files in place and would kill the playback
    #[tokio::test]
    #[test_log::test]
    async fn boost_never_touches_the_playing_episode() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(low_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        actor_with_boost(&fake, 3, true, boost_rule(&["42"], "HQ-1080p", 3))
            .prefetch(boosted_np())
            .await?;

        // s01e05 is playing
        assert!(!fake.episode(15)["monitored"].as_bool().unwrap());
        for command in fake.commands() {
            let ids = command["episodeIds"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            assert!(
                !ids.contains(&json!(15)),
                "the playing episode must not be searched: {command}"
            );
        }
        Ok(())
    }

    // A boosted series is never season-searched, even with request_seasons on:
    // a season pack upgrade would replace the file being streamed
    #[tokio::test]
    #[test_log::test]
    async fn boost_forces_episode_searches() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        // Missing files, fully aired seasons — without a boost this is exactly
        // the case that produces a SeasonSearch.
        fake.add_episodes(default_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 0));

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(boosted_np())
            .await?;

        for command in fake.commands() {
            assert_eq!(
                command["name"].as_str(),
                Some("EpisodeSearch"),
                "boosted series must not be season-searched: {command}"
            );
        }
        Ok(())
    }

    // Episodes already in the download queue are not searched again
    #[tokio::test]
    #[test_log::test]
    async fn boost_skips_queued_episodes() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(low_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));
        fake.enqueue(1234, 16);

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(boosted_np())
            .await?;

        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [17]})]
        );
        Ok(())
    }

    // An existing tag is reused rather than duplicated, and other tags survive
    #[tokio::test]
    #[test_log::test]
    async fn boost_reuses_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_tag(1, "hq-boosted");
        let mut series = default_series();
        series["tags"] = json!([7]);
        fake.add_series(series);
        fake.add_episodes(low_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(boosted_np())
            .await?;

        assert_eq!(fake.tags().len(), 1);
        assert_eq!(fake.series_state(1234)["tags"], json!([7, 1]));
        Ok(())
    }

    // A user without a matching rule keeps the unmodified behaviour: season
    // searches, no profile change, no tag
    #[tokio::test]
    #[test_log::test]
    async fn unboosted_user_unaffected() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        let series = fake.series_state(1234);
        assert_eq!(series["qualityProfileId"].as_i64(), Some(1));
        assert!(series["tags"].is_null());
        assert!(fake.tags().is_empty());
        assert_eq!(
            fake.commands(),
            vec![
                json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 1}),
                json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 2}),
            ]
        );
        Ok(())
    }

    // An unboosted viewer of an already-boosted series also gets episode
    // searches: a season pack grabbed under the boosted profile would replace
    // the file they are streaming
    #[tokio::test]
    #[test_log::test]
    async fn already_boosted_series_skips_season_search() -> Result<(), Box<dyn std::error::Error>>
    {
        let fake = FakeSonarr::start().await;
        fake.add_tag(1, "hq-boosted");
        let mut series = default_series();
        series["tags"] = json!([1]);
        fake.add_series(series);
        fake.add_episodes(default_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        // The viewer has no rule of their own; the series carries the tag from
        // someone else's session.
        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 5,
                season: 1,
                ..np_default()
            })
            .await?;

        for command in fake.commands() {
            assert_eq!(
                command["name"].as_str(),
                Some("EpisodeSearch"),
                "boosted series must not be season-searched: {command}"
            );
        }
        // The profile is left alone for a user without a rule
        assert_eq!(
            fake.series_state(1234)["qualityProfileId"].as_i64(),
            Some(1)
        );
        Ok(())
    }

    // A missing episode inside the boost window is fetched once, not searched
    // by both the fetch and the upgrade pass
    #[tokio::test]
    #[test_log::test]
    async fn boost_does_not_double_search() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        // Not fully aired, so the fetch pass searches episodes rather than
        // seasons and both passes cover the same ids.
        let mut series = default_series();
        series["seasons"][1] = make_season(1, false, false);
        fake.add_series(series);
        fake.add_episodes(default_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2))
            .prefetch(boosted_np())
            .await?;

        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [16, 17]})]
        );
        Ok(())
    }

    // A series held by a second instance is found there
    #[tokio::test]
    #[test_log::test]
    async fn routes_to_the_instance_holding_the_series() -> Result<(), Box<dyn std::error::Error>> {
        let empty = FakeSonarr::start().await;
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor_with_instances(vec![instance(&empty), instance(&fake)], 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(empty.commands().is_empty());
        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // The library decides which instance handles a session, even when both
    // hold a series of that name
    #[tokio::test]
    #[test_log::test]
    async fn routes_by_library() -> Result<(), Box<dyn std::error::Error>> {
        let tv = FakeSonarr::start().await;
        tv.add_series(default_series());
        tv.add_episodes(default_episodes());

        let anime = FakeSonarr::start().await;
        anime.add_series(default_series());
        anime.add_episodes(default_episodes());

        let instances = vec![
            instance_with(&tv, vec!["Television"], None, Vec::new()),
            instance_with(&anime, vec!["Anime"], None, Vec::new()),
        ];

        actor_with_instances(instances, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                library: Some("Anime".to_string()),
                ..np_default()
            })
            .await?;

        assert!(tv.commands().is_empty());
        assert!(!anime.commands().is_empty());
        Ok(())
    }

    // With no library filters, the first instance holding the series wins
    #[tokio::test]
    #[test_log::test]
    async fn first_matching_instance_wins() -> Result<(), Box<dyn std::error::Error>> {
        let first = FakeSonarr::start().await;
        first.add_series(default_series());
        first.add_episodes(default_episodes());

        let second = FakeSonarr::start().await;
        second.add_series(default_series());
        second.add_episodes(default_episodes());

        actor_with_instances(vec![instance(&first), instance(&second)], 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!first.commands().is_empty());
        assert!(second.commands().is_empty());
        Ok(())
    }

    // Boost rules are per instance: the same user is boosted on one and not on
    // the other
    #[tokio::test]
    #[test_log::test]
    async fn boost_rules_are_per_instance() -> Result<(), Box<dyn std::error::Error>> {
        let tv = FakeSonarr::start().await;
        tv.add_series(default_series());
        tv.add_episodes(low_quality_episodes());

        let anime = FakeSonarr::start().await;
        anime.add_series(default_series());
        anime.add_episodes(low_quality_episodes());
        anime.add_quality_profile(make_quality_profile(9, "HQ-Anime", 1000));

        let instances = vec![
            instance_with(&tv, vec!["Television"], None, Vec::new()),
            instance_with(
                &anime,
                vec!["Anime"],
                None,
                vec![boost_rule(&["42"], "HQ-Anime", 2)],
            ),
        ];

        actor_with_instances(instances, 2, true)
            .prefetch(NowPlaying {
                library: Some("Anime".to_string()),
                ..boosted_np()
            })
            .await?;

        assert_eq!(
            anime.series_state(1234)["qualityProfileId"].as_i64(),
            Some(9)
        );
        assert_eq!(tv.series_state(1234)["qualityProfileId"].as_i64(), Some(1));
        assert!(tv.commands().is_empty());
        Ok(())
    }

    // A quality profile that does not exist in Sonarr fails the boost loudly
    // instead of silently prefetching at the old quality
    #[tokio::test]
    #[test_log::test]
    async fn boost_unknown_quality_profile_errors() {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(low_quality_episodes());

        let result = actor_with_boost(&fake, 2, true, boost_rule(&["42"], "Nonexistent", 2))
            .prefetch(boosted_np())
            .await;

        assert!(result.is_err());
        assert!(fake.commands().is_empty());
    }

    // A dry run reaches the end of the boost path without writing anything to
    // Sonarr
    #[tokio::test]
    #[test_log::test]
    async fn dry_run_makes_no_writes() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(low_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        let instance = super::Instance::new(
            "test".to_string(),
            crate::sonarr::Client::new(fake.url(), "secret", true).unwrap(),
            Vec::new(),
            None,
            vec![boost_rule(&["42"], "HQ-1080p", 2)],
        );

        actor_with_instances(vec![instance], 2, true)
            .prefetch(boosted_np())
            .await?;

        let series = fake.series_state(1234);
        assert_eq!(series["qualityProfileId"].as_i64(), Some(1));
        assert!(series["tags"].is_null());
        assert!(fake.tags().is_empty());
        assert!(fake.commands().is_empty());
        assert!(!fake.episode(16)["monitored"].as_bool().unwrap());
        Ok(())
    }

    // Re-triggering the same episode for the same user is a no-op
    #[tokio::test]
    #[test_log::test]
    async fn boost_deduplicates() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(low_quality_episodes());
        fake.add_quality_profile(make_quality_profile(9, "HQ-1080p", 1000));

        let mut actor = actor_with_boost(&fake, 2, true, boost_rule(&["42"], "HQ-1080p", 2));
        actor.prefetch(boosted_np()).await?;
        actor.prefetch(boosted_np()).await?;

        assert_eq!(fake.commands().len(), 1);
        Ok(())
    }
}
