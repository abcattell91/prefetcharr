use std::{collections::HashSet, time::Duration};

use tracing::debug;

use crate::{
    config,
    media_server::User,
    sonarr::{EpisodeResource, QualityProfileResource},
    util::time::parse_utc_timestamp,
};

/// A per-user rule that upgrades upcoming episodes instead of merely fetching
/// the missing ones.
#[derive(Clone, Debug)]
pub struct Rule {
    users: Vec<String>,
    pub quality_profile: String,
    pub prefetch_num: usize,
    pub tag: String,
    pub search_cooldown: Duration,
}

impl From<config::Boost> for Rule {
    fn from(boost: config::Boost) -> Self {
        Self {
            users: boost.users,
            quality_profile: boost.quality_profile,
            prefetch_num: boost.prefetch_num,
            tag: boost.tag,
            search_cooldown: Duration::from_secs(boost.search_cooldown),
        }
    }
}

impl Rule {
    pub fn matches(&self, user: &User) -> bool {
        // An empty list here means "nobody" rather than "everybody": a boost
        // rule without users is a configuration mistake, and defaulting it to
        // all users would silently upgrade the whole library.
        matches_user(&self.users, user)
    }
}

/// Whether `user` is named by `wanted`, by ID or by name.
///
/// Shared with the media server session filter so the two ways of naming a
/// user in the configuration cannot drift apart.
pub fn matches_user(wanted: &[String], user: &User) -> bool {
    wanted.contains(&user.id) || wanted.contains(&user.name)
}

/// Pick the episodes worth searching for out of an upcoming-episode window.
///
/// The caller must have applied the boosted quality profile to the series
/// *before* fetching `episodes`: `quality_cutoff_not_met` is computed against
/// the series' current profile, so a list read under the old profile reports
/// every existing file as good enough and this returns nothing.
pub fn select_upgrades(
    episodes: &[EpisodeResource],
    profile: &QualityProfileResource,
    queued: &HashSet<i32>,
    now: i64,
    cooldown: Duration,
) -> Vec<EpisodeResource> {
    let cooldown = i64::try_from(cooldown.as_secs()).unwrap_or(i64::MAX);

    episodes
        .iter()
        .filter(|e| {
            let wanted = needs_upgrade(e, profile);
            if !wanted {
                debug!(id = e.id, "already at the target quality");
            }
            wanted
        })
        .filter(|e| {
            let queued = queued.contains(&e.id);
            if queued {
                debug!(id = e.id, "already downloading");
            }
            !queued
        })
        .filter(|e| {
            // An episode that hasn't aired cannot be found by any indexer.
            let aired = e
                .air_date_utc
                .as_deref()
                .and_then(parse_utc_timestamp)
                .is_none_or(|air_date| air_date <= now);
            if !aired {
                debug!(id = e.id, "has not aired yet");
            }
            aired
        })
        .filter(|e| {
            // Repeatedly searching an episode no indexer can satisfy burns the
            // daily API budget for nothing.
            let recent = e
                .last_search_time
                .as_deref()
                .and_then(parse_utc_timestamp)
                .is_some_and(|last| now.saturating_sub(last) < cooldown);
            if recent {
                debug!(id = e.id, "searched recently, skipping");
            }
            !recent
        })
        .cloned()
        .collect()
}

fn needs_upgrade(episode: &EpisodeResource, profile: &QualityProfileResource) -> bool {
    let Some(file) = &episode.episode_file else {
        // Without a file there is nothing to upgrade — but there may be
        // something to fetch.
        return !episode.has_file;
    };

    if file.quality_cutoff_not_met {
        return true;
    }

    // "Upgrade Until Custom Format Score" is a second cutoff, independent of
    // the quality one. With a TRaSH-style profile it is what actually drives
    // upgrades, so an episode can be quality-cutoff-met and still far below
    // the target.
    match (file.custom_format_score, profile.cutoff_format_score) {
        (Some(score), Some(cutoff)) if cutoff > 0 => score < cutoff,
        _ => false,
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, time::Duration};

    use crate::{
        media_server::User,
        sonarr::{EpisodeFileResource, EpisodeResource, QualityProfileResource},
    };

    const NOW: i64 = 1_735_689_600; // 2025-01-01T00:00:00Z

    fn profile(cutoff_format_score: Option<i32>) -> QualityProfileResource {
        QualityProfileResource {
            id: 7,
            name: "HQ-1080p".to_string(),
            cutoff_format_score,
            upgrade_allowed: Some(true),
        }
    }

    fn episode(id: i32, has_file: bool, file: Option<&EpisodeFileResource>) -> EpisodeResource {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "seasonNumber": 1,
            "episodeNumber": id,
            "hasFile": has_file,
            "monitored": false,
            "airDateUtc": "2024-01-01T00:00:00Z",
            "episodeFile": file,
        }))
        .unwrap()
    }

    fn file(quality_cutoff_not_met: bool, custom_format_score: Option<i32>) -> EpisodeFileResource {
        serde_json::from_value(serde_json::json!({
            "qualityCutoffNotMet": quality_cutoff_not_met,
            "customFormatScore": custom_format_score,
        }))
        .unwrap()
    }

    fn select(episodes: &[EpisodeResource], profile: &QualityProfileResource) -> Vec<i32> {
        super::select_upgrades(
            episodes,
            profile,
            &HashSet::new(),
            NOW,
            Duration::from_secs(0),
        )
        .iter()
        .map(|e| e.id)
        .collect()
    }

    // Episodes without a file are selected so they get fetched
    #[test]
    fn missing_file_selected() {
        let episodes = [episode(1, false, None)];
        assert_eq!(select(&episodes, &profile(None)), [1]);
    }

    // A file already at the profile's cutoff is left alone
    #[test]
    fn cutoff_met_skipped() {
        let episodes = [episode(1, true, Some(&file(false, None)))];
        assert!(select(&episodes, &profile(None)).is_empty());
    }

    // A file below the quality cutoff is selected for upgrade
    #[test]
    fn cutoff_not_met_selected() {
        let episodes = [episode(1, true, Some(&file(true, None)))];
        assert_eq!(select(&episodes, &profile(None)), [1]);
    }

    // Custom format score below the profile's target selects the episode even
    // when the quality cutoff is met
    #[test]
    fn custom_format_score_below_cutoff() {
        let episodes = [episode(1, true, Some(&file(false, Some(100))))];
        assert_eq!(select(&episodes, &profile(Some(500))), [1]);
    }

    // Custom format score at or above the target is good enough
    #[test]
    fn custom_format_score_at_cutoff() {
        let episodes = [episode(1, true, Some(&file(false, Some(500))))];
        assert!(select(&episodes, &profile(Some(500))).is_empty());
    }

    // A profile that doesn't use custom format scoring ignores the score
    #[test]
    fn custom_format_scoring_disabled() {
        let episodes = [episode(1, true, Some(&file(false, Some(0))))];
        assert!(select(&episodes, &profile(Some(0))).is_empty());
    }

    // Episodes already in the download queue are not searched again
    #[test]
    fn queued_skipped() {
        let episodes = [episode(1, false, None), episode(2, false, None)];
        let queued: HashSet<i32> = [1].into_iter().collect();
        let selected: Vec<i32> =
            super::select_upgrades(&episodes, &profile(None), &queued, NOW, Duration::ZERO)
                .iter()
                .map(|e| e.id)
                .collect();
        assert_eq!(selected, [2]);
    }

    // Episodes that have not aired yet are skipped
    #[test]
    fn unaired_skipped() {
        let mut unaired = episode(1, false, None);
        unaired.air_date_utc = Some("2099-01-01T00:00:00Z".to_string());
        assert!(select(&[unaired], &profile(None)).is_empty());
    }

    // An episode with no air date at all is still eligible
    #[test]
    fn missing_air_date_selected() {
        let mut episode = episode(1, false, None);
        episode.air_date_utc = None;
        assert_eq!(select(&[episode], &profile(None)), [1]);
    }

    // An episode searched within the cooldown window is skipped, one searched
    // before it is not
    #[test]
    fn search_cooldown() {
        let mut recent = episode(1, false, None);
        recent.last_search_time = Some("2024-12-31T23:00:00Z".to_string()); // 1h ago
        let mut old = episode(2, false, None);
        old.last_search_time = Some("2024-12-30T00:00:00Z".to_string()); // 2d ago

        let episodes = [recent, old];
        let cooldown = Duration::from_secs(60 * 60 * 12);
        let selected: Vec<i32> =
            super::select_upgrades(&episodes, &profile(None), &HashSet::new(), NOW, cooldown)
                .iter()
                .map(|e| e.id)
                .collect();
        assert_eq!(selected, [2]);
    }

    // Never-searched episodes are unaffected by the cooldown
    #[test]
    fn never_searched_ignores_cooldown() {
        let episodes = [episode(1, false, None)];
        let cooldown = Duration::from_secs(60 * 60 * 12);
        let selected =
            super::select_upgrades(&episodes, &profile(None), &HashSet::new(), NOW, cooldown);
        assert_eq!(selected.len(), 1);
    }

    // Rules match users by name or by ID, and reject everyone else
    #[test]
    fn rule_matches_user() {
        let rule = super::Rule::from(crate::config::Boost {
            users: vec!["Hans".to_string(), "42".to_string()],
            quality_profile: "HQ".to_string(),
            prefetch_num: 5,
            tag: "hq".to_string(),
            search_cooldown: 0,
        });

        assert!(rule.matches(&User {
            name: "Hans".to_string(),
            id: "1".to_string()
        }));
        assert!(rule.matches(&User {
            name: "Other".to_string(),
            id: "42".to_string()
        }));
        assert!(!rule.matches(&User {
            name: "Nobody".to_string(),
            id: "7".to_string()
        }));
    }

    // A rule naming no users matches nobody rather than everybody
    #[test]
    fn empty_rule_matches_nobody() {
        let rule = super::Rule::from(crate::config::Boost {
            users: vec![],
            quality_profile: "HQ".to_string(),
            prefetch_num: 5,
            tag: "hq".to_string(),
            search_cooldown: 0,
        });

        assert!(!rule.matches(&User {
            name: "Hans".to_string(),
            id: "1".to_string()
        }));
    }
}
