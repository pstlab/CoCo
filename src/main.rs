#[cfg(feature = "fcm")]
use coco::fcm::{fcm_router, setup_fcm};
#[cfg(feature = "ollama")]
use coco::kb::clips::ollama::setup_ollama;
#[cfg(feature = "mqtt")]
use coco::mqtt::setup_mqtt;
use coco::{CoCo, db::setup_mongodb, kb::setup_clips, server::coco_router};
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;
use tracing::{Level, error, subscriber};

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    subscriber::set_global_default(subscriber).expect("Failed to set global default subscriber");

    let db = setup_mongodb().await.unwrap_or_else(|e| {
        error!("Failed to set up MongoDB: {}", e);
        std::process::exit(1);
    });

    let kb = setup_clips().unwrap_or_else(|e| {
        error!("Failed to set up knowledge base: {}", e);
        std::process::exit(1);
    });

    #[cfg(feature = "ollama")]
    setup_ollama(&kb).await.unwrap_or_else(|e| {
        error!("Failed to set up Ollama integration: {}", e);
        std::process::exit(1);
    });

    #[cfg(feature = "fcm")]
    setup_fcm(db.clone(), &kb).await.unwrap_or_else(|e| {
        error!("Failed to add FCM to knowledge base: {}", e);
        std::process::exit(1);
    });

    let coco = CoCo::new(db.clone(), kb).await;

    #[cfg(feature = "mqtt")]
    setup_mqtt(coco.clone()).await.unwrap_or_else(|e| {
        error!("Failed to set up MQTT integration: {}", e);
        std::process::exit(1);
    });

    let app = coco_router(coco).await;
    #[cfg(feature = "fcm")]
    let app = app.merge(fcm_router(db));
    let app = app.nest_service("/assets", ServeDir::new("gui/dist/assets")).fallback_service(ServeDir::new("gui/dist").not_found_service(ServeFile::new("gui/dist/index.html")));

    let port = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    info!("Starting CoCo server on port {}", port);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
