use std::{
    collections::HashSet,
    io::{BufReader, Read, Seek},
};

use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use quick_xml::{events::Event, Reader};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Transaction;

use crate::{
    apple_health_export::{attributes, PROVIDER},
    wearables::Record,
};

const MAX_GPX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_POINTS: usize = 100_000;

pub struct Pending {
    source: String,
    source_id: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    files: Vec<String>,
}

impl Pending {
    pub fn take(record: &mut Record) -> Option<Self> {
        let files =
            serde_json::from_value(record.payload.as_object_mut()?.remove("route_files")?).ok()?;
        Some(Self {
            source: record.source.clone(),
            source_id: record.source_id.clone(),
            start: record.started_at,
            end: record.ended_at,
            files,
        })
    }
}

fn archive_route(xml_name: &str, reference: &str) -> Option<String> {
    let relative = reference.strip_prefix('/').unwrap_or(reference);
    let filename = relative.strip_prefix("workout-routes/")?;
    if filename.len() > 255 || !filename.ends_with(".gpx") || filename.contains(['/', '\\', ':']) {
        return None;
    }
    let prefix = xml_name.rsplit_once('/').map_or("", |(prefix, _)| prefix);
    Some(if prefix.is_empty() {
        relative.to_owned()
    } else {
        format!("{prefix}/{relative}")
    })
}

#[derive(Serialize)]
struct Point {
    latitude: f64,
    longitude: f64,
    timestamp_ms: Option<i64>,
    altitude: Option<f64>,
    segment: usize,
}

fn coordinates(node: &quick_xml::events::BytesStart<'_>, segment: usize) -> Result<Option<Point>> {
    let fields = attributes(node)?;
    let parse = |name| fields.get(name).and_then(|value| value.parse::<f64>().ok());
    Ok(parse("lat")
        .zip(parse("lon"))
        .filter(|(lat, lon)| (-90.0..=90.0).contains(lat) && (-180.0..=180.0).contains(lon))
        .map(|(latitude, longitude)| Point {
            latitude,
            longitude,
            timestamp_ms: None,
            altitude: None,
            segment,
        }))
}

fn points(input: impl Read, route: &Pending, next_segment: &mut usize) -> Result<Vec<Point>> {
    let mut reader = Reader::from_reader(BufReader::new(input.take(MAX_GPX_BYTES + 1)));
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut path = Vec::new();
    let mut current = None::<Point>;
    let mut points = Vec::new();
    let mut done = false;
    loop {
        buf.clear();
        let event = reader.read_event_into(&mut buf)?;
        ensure!(
            reader.buffer_position() <= MAX_GPX_BYTES,
            "Apple workout route exceeds the size limit"
        );
        match event {
            Event::Start(node) => {
                ensure!(path.len() < 32, "invalid Apple workout route nesting");
                let tag = node.name().as_ref().to_owned();
                if path.is_empty() {
                    ensure!(!done && tag == "gpx", "invalid Apple workout route");
                } else if path == ["gpx", "trk"] && tag == "trkseg" {
                    *next_segment += 1;
                } else if path == ["gpx", "trk", "trkseg"] && tag == "trkpt" {
                    current = coordinates(&node, *next_segment)?;
                }
                path.push(tag);
            }
            Event::Empty(node)
                if path == ["gpx", "trk", "trkseg"] && node.name().as_ref() == "trkpt" =>
            {
                if let Some(point) = coordinates(&node, *next_segment)? {
                    ensure!(
                        points.len() < MAX_POINTS,
                        "too many points in Apple workout route"
                    );
                    points.push(point);
                }
            }
            Event::Text(value)
                if path.len() == 5 && path[..4] == ["gpx", "trk", "trkseg", "trkpt"] =>
            {
                if let Some(point) = &mut current {
                    match path[4].as_str() {
                        "time" => {
                            if let Ok(at) = DateTime::parse_from_rfc3339(value.as_ref()) {
                                point.timestamp_ms = Some(at.timestamp_millis());
                            }
                        }
                        "ele" => {
                            point.altitude =
                                value.as_ref().parse::<f64>().ok().filter(|v| v.is_finite())
                        }
                        _ => {}
                    }
                }
            }
            Event::End(_) => {
                if path == ["gpx", "trk", "trkseg", "trkpt"] {
                    if let Some(point) = current.take().filter(|point| {
                        point.timestamp_ms.is_none_or(|at| {
                            (route.start.timestamp_millis()..=route.end.timestamp_millis())
                                .contains(&at)
                        })
                    }) {
                        ensure!(
                            points.len() < MAX_POINTS,
                            "too many points in Apple workout route"
                        );
                        points.push(point);
                    }
                }
                path.pop();
                done |= path.is_empty();
            }
            Event::Eof => {
                ensure!(done && path.is_empty(), "incomplete Apple workout route");
                return Ok(points);
            }
            _ => {}
        }
    }
}

pub async fn import<R: Read + Seek>(
    tx: &Transaction<'_>,
    subject: &str,
    archive: &mut zip::ZipArchive<R>,
    xml_name: &str,
    route: Pending,
) -> Result<bool> {
    let mut stored = Vec::new();
    let mut segment = 0;
    let mut visited = HashSet::new();
    for reference in &route.files {
        let Some(name) =
            archive_route(xml_name, reference).filter(|name| visited.insert(name.clone()))
        else {
            continue;
        };
        let Ok(file) = archive.by_name(&name) else {
            continue;
        };
        if !file.is_file() || file.size() > MAX_GPX_BYTES {
            continue;
        }
        if let Ok(mut part) = points(file, &route, &mut segment) {
            if stored.len() + part.len() > MAX_POINTS {
                return Ok(false);
            }
            stored.append(&mut part);
        }
    }
    if stored.is_empty() {
        return Ok(false);
    }
    let points = serde_json::to_value(&stored)?;
    tx.execute(
        "INSERT INTO avatar_hsm.apple_export_workout_route(subject_id,source,source_id,points)
         VALUES($1,$2,$3,$4) ON CONFLICT(subject_id,source,source_id) DO UPDATE SET points=excluded.points",
        &[&subject,&route.source,&route.source_id,&points],
    ).await?;
    Ok(true)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    source: String,
    source_id: String,
    #[serde(default)]
    offset: i64,
    limit: Option<i64>,
}

pub async fn query(pool: &Pool, subject: &str, query: Query) -> Result<Value> {
    let limit = query.limit.unwrap_or(100);
    ensure!(
        [&query.source, &query.source_id]
            .into_iter()
            .all(|value| !value.trim().is_empty() && value.len() <= 256)
            && (0..=MAX_POINTS as i64).contains(&query.offset)
            && (1..=100).contains(&limit),
        "invalid Apple workout route query"
    );
    let db = pool.get().await?;
    let row = db.query_opt(
        "SELECT jsonb_array_length(r.points), COALESCE((SELECT jsonb_agg(value ORDER BY ord)
           FROM jsonb_array_elements(r.points) WITH ORDINALITY p(value,ord) WHERE ord>$4::bigint AND ord<=$4::bigint+$5::bigint),'[]'::jsonb)
         FROM avatar_hsm.apple_export_workout_route r JOIN avatar_hsm.device_connection c USING(subject_id,provider)
         WHERE r.subject_id=$1 AND r.source=$2 AND r.source_id=$3 AND c.state='synced' AND 'workout'=ANY(c.grants)",
        &[&subject,&query.source,&query.source_id,&query.offset,&limit],
    ).await?;
    let Some(row) = row else {
        return Ok(json!({"state":"not_found","records":[]}));
    };
    let total = i64::from(row.get::<_, i32>(0));
    let next = query.offset + limit;
    Ok(
        json!({"state":"ready","provider":PROVIDER,"source":query.source,"source_id":query.source_id,
        "stored_point_count":total,"next_offset":(next<total).then_some(next),"records":row.get::<_,Value>(1)}),
    )
}
