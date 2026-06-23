use crate::settings::Settings;
use anyhow::Context;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, instrument};

pub async fn run(settings: Settings) -> anyhow::Result<()> {

    let listener = TcpListener::bind(&settings.server.listen_addr)
        .await
        .with_context(|| format!("Failed to bind to {}", settings.server.listen_addr))?;

    info!(
        listen_addr = %settings.server.listen_addr,
        upstream_addr = %settings.upstream.addr,
        "Starting proxy server"
    );

    loop {
        let (client, client_addr) = listener.accept()
            .await
            .context("Failed to accept incoming connection")?;

        let settings = settings.clone();

        tokio::spawn(async move {
            if let Err(err) = handle_connection(client, settings).await {
                error!(
                    client_addr = %client_addr,
                    error = ?err,
                    "Connection failed"
                );
            }
        });
    }
}

#[instrument(skip(client, settings))]
async fn handle_connection(mut client: TcpStream, settings: Settings) -> anyhow::Result<()> {

    let mut upstream = TcpStream::connect(&settings.upstream.addr)
        .await
        .with_context(|| format!("Failed to connect to upstream {}", settings.upstream.addr))?;

    let bytes = io::copy_bidirectional(&mut client, &mut upstream)
        .await
        .with_context(|| format!("Failed to proxy traffic"))?;

    info!(
        client_to_upstream_bytes = bytes.0,
        upstream_to_client_bytes = bytes.1,
        "Connection closed"
    );

    Ok(())
}