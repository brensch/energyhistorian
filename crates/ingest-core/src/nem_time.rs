use chrono::{DateTime, Duration, NaiveDate, Utc};

const NEM_OFFSET_HOURS: i64 = 10;

pub fn looks_like_nem_date(value: &str) -> bool {
    value.len() == 10
        && matches!(value.as_bytes().get(4), Some(b'-' | b'/'))
        && matches!(value.as_bytes().get(7), Some(b'-' | b'/'))
}

pub fn looks_like_nem_datetime(value: &str) -> bool {
    value.len() >= 19
        && matches!(value.as_bytes().get(4), Some(b'-' | b'/'))
        && matches!(value.as_bytes().get(7), Some(b'-' | b'/'))
        && matches!(value.as_bytes().get(10), Some(b' ' | b'T'))
}

pub fn parse_nem_date(value: &str) -> Option<NaiveDate> {
    if !looks_like_nem_date(value) {
        return None;
    }

    let year = parse_u32(value, 0, 4)? as i32;
    let month = parse_u32(value, 5, 7)?;
    let day = parse_u32(value, 8, 10)?;
    NaiveDate::from_ymd_opt(year, month, day)
}

pub fn parse_nem_datetime(value: &str) -> Option<DateTime<Utc>> {
    if !looks_like_nem_datetime(value) {
        return None;
    }

    let date = parse_nem_date(&value[..10])?;
    let hour = parse_u32(value, 11, 13)?;
    let minute = parse_u32(value, 14, 16)?;
    let second = parse_u32(value, 17, 19)?;
    let millis = parse_fractional_millis(value.get(19..))?;
    let naive = date.and_hms_milli_opt(hour, minute, second, millis)?;
    let utc = naive - Duration::hours(NEM_OFFSET_HOURS);
    Some(DateTime::<Utc>::from_naive_utc_and_offset(utc, Utc))
}

fn parse_fractional_millis(suffix: Option<&str>) -> Option<u32> {
    let Some(suffix) = suffix else {
        return Some(0);
    };
    if suffix.is_empty() {
        return Some(0);
    }

    let bytes = suffix.as_bytes();
    if bytes.first().copied()? != b'.' {
        return None;
    }

    let digits = &suffix[1..];
    if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let mut millis = 0u32;
    for (idx, byte) in digits.bytes().take(3).enumerate() {
        let digit = u32::from(byte - b'0');
        millis += digit * 10u32.pow(2 - idx as u32);
    }

    Some(millis)
}

fn parse_u32(value: &str, start: usize, end: usize) -> Option<u32> {
    value.get(start..end)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone};

    use super::{parse_nem_date, parse_nem_datetime};

    #[test]
    fn parses_nem_date() {
        assert_eq!(
            parse_nem_date("2026/03/12"),
            Some(NaiveDate::from_ymd_opt(2026, 3, 12).unwrap())
        );
    }

    #[test]
    fn parses_nem_datetime_to_utc() {
        let parsed = parse_nem_datetime("2026/03/12 00:05:00").unwrap();
        assert_eq!(
            parsed,
            chrono::Utc.with_ymd_and_hms(2026, 3, 11, 14, 5, 0).unwrap()
        );
    }

    #[test]
    fn parses_fractional_nem_datetime_to_utc() {
        let parsed = parse_nem_datetime("2026-03-12T00:05:00.1234").unwrap();
        assert_eq!(
            parsed,
            chrono::Utc.with_ymd_and_hms(2026, 3, 11, 14, 5, 0).unwrap()
                + chrono::Duration::milliseconds(123)
        );
    }
}
