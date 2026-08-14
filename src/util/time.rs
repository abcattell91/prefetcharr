use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since the Unix epoch, or `None` before it.
pub fn now() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

/// Parse a UTC timestamp as returned by Sonarr into seconds since the Unix
/// epoch.
///
/// Sonarr serializes `DateTime` values as `YYYY-MM-DDTHH:MM:SSZ`, sometimes
/// with fractional seconds and sometimes without the trailing `Z`. Anything
/// else — including timestamps before the epoch — yields `None`, which callers
/// treat as "unknown" rather than as an error.
pub fn parse_utc_timestamp(s: &str) -> Option<i64> {
    let (date, time) = s.split_once('T')?;

    let mut date = date.splitn(3, '-');
    let year: i64 = date.next()?.parse().ok()?;
    let month: u32 = date.next()?.parse().ok()?;
    let day: i64 = date.next()?.parse().ok()?;

    // Drop the timezone marker and any fractional seconds; Sonarr only ever
    // emits UTC here.
    let time = time.trim_end_matches('Z');
    let time = time.split_once('.').map_or(time, |(t, _)| t);
    let mut time = time.splitn(3, ':');
    let hour: i64 = time.next()?.parse().ok()?;
    let minute: i64 = time.next()?.parse().ok()?;
    let second: i64 = time.next().unwrap_or("0").parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let days = days_from_civil(year, month, day);
    Some(days * 86400 + hour * 3600 + minute * 60 + second)
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
///
/// Howard Hinnant's `days_from_civil`, which is exact for any year in range
/// and avoids pulling in a date library for the one thing we need it for.
fn days_from_civil(year: i64, month: u32, day: i64) -> i64 {
    let month = i64::from(month);
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod test {
    use super::parse_utc_timestamp;

    // The epoch itself is zero
    #[test]
    fn epoch() {
        assert_eq!(parse_utc_timestamp("1970-01-01T00:00:00Z"), Some(0));
    }

    // A known timestamp round-trips to the expected epoch seconds
    #[test]
    fn known_timestamps() {
        assert_eq!(
            parse_utc_timestamp("2025-01-01T00:00:00Z"),
            Some(1_735_689_600)
        );
        assert_eq!(
            parse_utc_timestamp("2024-02-29T12:34:56Z"),
            Some(1_709_210_096)
        );
        assert_eq!(
            parse_utc_timestamp("2000-03-01T00:00:00Z"),
            Some(951_868_800)
        );
    }

    // Fractional seconds and a missing `Z` are both accepted
    #[test]
    fn optional_parts() {
        let expected = Some(1_735_689_600);
        assert_eq!(parse_utc_timestamp("2025-01-01T00:00:00"), expected);
        assert_eq!(parse_utc_timestamp("2025-01-01T00:00:00.123Z"), expected);
        assert_eq!(
            parse_utc_timestamp("2025-01-01T00:00:00.1234567Z"),
            expected
        );
    }

    // Leap years are handled by the civil-date conversion
    #[test]
    fn leap_years() {
        // 2000 is a leap year, 1900 is not — the century rule must apply.
        let feb29_2000 = parse_utc_timestamp("2000-02-29T00:00:00Z").unwrap();
        let mar01_2000 = parse_utc_timestamp("2000-03-01T00:00:00Z").unwrap();
        assert_eq!(mar01_2000 - feb29_2000, 86400);

        let feb28_1900 = parse_utc_timestamp("1900-02-28T00:00:00Z").unwrap();
        let mar01_1900 = parse_utc_timestamp("1900-03-01T00:00:00Z").unwrap();
        assert_eq!(mar01_1900 - feb28_1900, 86400);
    }

    // Dates before the epoch are negative rather than wrapping
    #[test]
    fn before_epoch() {
        assert_eq!(parse_utc_timestamp("1969-12-31T00:00:00Z"), Some(-86400));
    }

    // Consecutive days are exactly one day apart across a month boundary
    #[test]
    fn month_boundary() {
        let last = parse_utc_timestamp("2025-01-31T00:00:00Z").unwrap();
        let first = parse_utc_timestamp("2025-02-01T00:00:00Z").unwrap();
        assert_eq!(first - last, 86400);
    }

    // Malformed input yields None instead of a bogus timestamp
    #[test]
    fn malformed() {
        assert_eq!(parse_utc_timestamp(""), None);
        assert_eq!(parse_utc_timestamp("2025-01-01"), None);
        assert_eq!(parse_utc_timestamp("not a timestamp"), None);
        assert_eq!(parse_utc_timestamp("2025-13-01T00:00:00Z"), None);
        assert_eq!(parse_utc_timestamp("2025-01-32T00:00:00Z"), None);
        assert_eq!(parse_utc_timestamp("2025-01-01T25:00:00Z"), None);
        assert_eq!(parse_utc_timestamp("2025-01-01T00:61:00Z"), None);
        assert_eq!(parse_utc_timestamp("abcd-01-01T00:00:00Z"), None);
    }
}
