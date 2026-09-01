//! `humaux-gateway` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。

use std::{error::Error, net::SocketAddr};

use humaux_gateway::bootstrap::GatewayBootstrap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = GatewayBootstrap::load_from_env()?.build().await?;
    let listener = tokio::net::TcpListener::bind(runtime.bind_addr()).await?;
    let app = runtime.adapter().router();

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown = async move {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };

    eprintln!(
        "humaux-gateway listening on {} config_fingerprint={}",
        listener.local_addr()?,
        runtime.config_fingerprint()
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await?;
    Ok(())
}
