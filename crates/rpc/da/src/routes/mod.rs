use actix_web::web::{ServiceConfig, scope};

use crate::handlers::ingest::post_ingest;

/// Register every DA API route.
///
/// Everything versioned lives under the `/da/v0` scope so the prefix can evolve
/// independently. Add new handlers (retention, availability, column serving) to
/// [`register_v0_routes`].
pub fn register_routers(config: &mut ServiceConfig) {
    config.service(scope("/da/v0").configure(register_v0_routes));
}

/// Routes served under the `/da/v0` scope.
fn register_v0_routes(config: &mut ServiceConfig) {
    config.service(post_ingest);
}
