use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};

use crate::CONTROL_PLANE_BOOTSTRAP_SQL;

pub async fn connect_postgres(postgres_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .with_context(|| format!("connecting to postgres at {postgres_url}"))?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(error = ?error, "postgres connection task exited");
        }
    });

    Ok(client)
}

pub async fn bootstrap_postgres(postgres_url: &str) -> Result<Client> {
    let client = connect_postgres(postgres_url).await?;
    client
        .batch_execute(CONTROL_PLANE_BOOTSTRAP_SQL)
        .await
        .context("bootstrapping control-plane postgres schema")?;
    Ok(client)
}
