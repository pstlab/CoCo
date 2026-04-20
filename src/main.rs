use coco::CoCoModule;
use coco::db::mongodb::MongoDB;
#[cfg(feature = "fcm")]
use coco::fcm::{FCMModule, fcm_router};
use coco::kb::clips::CLIPSKnowledgeBase;
#[cfg(feature = "ollama")]
use coco::kb::clips::ollama::OllamaModule;
#[cfg(feature = "mqtt")]
use coco::mqtt::MQTTModule;
use coco::{CoCo, server::coco_router};
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;
use tracing::{Level, error, subscriber};

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    subscriber::set_global_default(subscriber).expect("Failed to set global default subscriber");

    let db = MongoDB::default().await.unwrap_or_else(|e| {
        error!("Failed to set up MongoDB: {}", e);
        std::process::exit(1);
    });
    let kb = CLIPSKnowledgeBase::default();
    let modules: Vec<Box<dyn CoCoModule<MongoDB, CLIPSKnowledgeBase>>> = vec![
        #[cfg(feature = "ollama")]
        Box::new(OllamaModule::default()),
        #[cfg(feature = "fcm")]
        Box::new(FCMModule::default()),
        #[cfg(feature = "mqtt")]
        Box::new(MQTTModule::default()),
    ];

    let coco = CoCo::new(db.clone(), kb, modules).await;

    let app = coco_router(coco).await;
    #[cfg(feature = "fcm")]
    let app = app.merge(fcm_router(db));
    let app = app.nest_service("/assets", ServeDir::new("gui/assets")).fallback_service(ServeDir::new("gui").not_found_service(ServeFile::new("gui/index.html")));

    let port = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    info!("Starting CoCo server on port {}", port);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
