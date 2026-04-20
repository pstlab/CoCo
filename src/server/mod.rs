use crate::CoCo;
#[cfg(feature = "secure")]
use crate::server::secure::{UsersDB, secure_coco_router};
#[cfg(not(feature = "secure"))]
use crate::server::unsecure::unsecure_coco_router;
use axum::Router;
use tower_http::services::{ServeDir, ServeFile};
use tracing::{error, info};

#[cfg(feature = "secure")]
pub mod secure;
#[cfg(not(feature = "secure"))]
pub mod unsecure;

pub async fn start_server(router: Router) {
    let app = router.nest_service("/assets", ServeDir::new("gui/dist/assets")).fallback_service(ServeDir::new("gui/dist").not_found_service(ServeFile::new("gui/dist/index.html")));

    let port = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    info!("Starting CoCo server on port {}", port);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub async fn coco_router(coco: CoCo) -> Router {
    #[cfg(feature = "secure")]
    return secure_coco_router(
        coco,
        UsersDB::default().await.unwrap_or_else(|e| {
            error!("Failed to set up users database: {}", e);
            std::process::exit(1);
        }),
    )
    .await;

    #[cfg(not(feature = "secure"))]
    return unsecure_coco_router(coco).await;
}
