use axum::{
    extract::DefaultBodyLimit,
    routing::{get, head, post},
    Router,
};
use std::sync::Arc;

use crate::{
    garmin_push, google_health_webhooks, google_workout_tcx, mcp, wearable_fit, wearable_oauth,
    wearable_webhooks,
};

/// HSM-owned provider callbacks and OAuth/download routes hosted by Watch
/// Tower. The legacy HSM MCP routes and test fixture routes are intentionally
/// not mounted here; clients use the unified Watch Tower `/mcp` endpoint.
pub(crate) fn router(app: Arc<mcp::App>) -> Router {
    routes(false, app)
}

#[cfg(test)]
pub(crate) fn test_router(app: Arc<mcp::App>) -> Router {
    routes(true, app)
}

fn routes(include_legacy_test_routes: bool, app: Arc<mcp::App>) -> Router {
    let router = Router::new()
        .route(
            "/wearables/webhooks/garmin/{key}",
            post(garmin_push::receive).layer(DefaultBodyLimit::max(garmin_push::MAX_BODY)),
        )
        .route(
            "/wearables/webhooks/coros",
            get(wearable_webhooks::coros_probe).post(wearable_webhooks::coros_event),
        )
        .route(
            "/wearables/webhooks/withings/{key}",
            head(wearable_webhooks::withings_probe).post(wearable_webhooks::withings_event),
        )
        .route(
            "/wearables/webhooks/suunto/{key}",
            post(wearable_webhooks::suunto_event),
        )
        .route(
            "/wearables/webhooks/google_health",
            post(google_health_webhooks::receive),
        )
        .route(
            "/wearables/webhooks/intervals_icu",
            post(wearable_webhooks::intervals_event),
        )
        .route(
            "/wearables/webhooks/{provider}",
            get(wearable_webhooks::oura_challenge).post(wearable_webhooks::signed_event),
        )
        .route(
            "/wearables/webhooks/strava/{capability}",
            get(wearable_webhooks::strava_challenge).post(wearable_webhooks::strava_event),
        )
        .route(
            "/wearables/oauth/{provider}/callback",
            get(wearable_oauth::callback),
        )
        .route("/wearables/connect", get(wearable_oauth::portal))
        .route("/wearables/portal.js", get(wearable_oauth::portal_script))
        .route("/wearables/portal.css", get(wearable_oauth::portal_style))
        .route(
            "/wearables/fit/{provider}/{activity}",
            get(wearable_fit::user_download),
        )
        .route(
            "/wearables/tcx/google_health/{activity}",
            get(google_workout_tcx::user_download),
        );
    let router = if include_legacy_test_routes {
        router
            .route(
                "/mcp/{surface}",
                post(mcp::handle).layer(DefaultBodyLimit::max(6 * 1024 * 1024)),
            )
            .route(
                "/internal/wearables/fit/{provider}/{activity}",
                get(wearable_fit::fixture_download),
            )
    } else {
        router
    };
    router.with_state(app)
}
