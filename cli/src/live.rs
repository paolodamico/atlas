//! Interactive live editing of a note through an `atlas-relay`.

use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow};
use atlas_client::{Client, Event};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast::Receiver;

/// Runs the live editor on its own runtime.
///
/// # Errors
/// Propagates client, relay, and I/O failures.
pub fn run(dir: &Path, note: &str, relay: &str, graph: &str) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(session(dir, note, relay, graph))
}

async fn session(dir: &Path, note: &str, relay: &str, graph: &str) -> Result<()> {
    let client = Client::open(dir)?;
    let mut events = client.subscribe();
    client.connect(relay, graph);

    let id = resolve(&client, &mut events, note).await?;
    render(&client, &id)?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        tokio::select! {
            event = events.recv() => {
                if let Ok(event) = event && concerns(&event, &id) {
                    render(&client, &id)?;
                }
            }
            line = lines.next_line() => {
                match line? {
                    Some(text) => {
                        append(&client, &id, &text)?;
                        render(&client, &id)?;
                    }
                    None => break,
                }
            }
        }
    }
    Ok(())
}

/// Resolves `token` (id, id prefix, or path) to a note id, waiting briefly for
/// it to arrive over the relay if it is not present yet.
async fn resolve(client: &Client, events: &mut Receiver<Event>, token: &str) -> Result<String> {
    if let Some(id) = find(client, token) {
        return Ok(id);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = events.recv().await;
            if let Some(id) = find(client, token) {
                return id;
            }
        }
    })
    .await
    .map_err(|_| anyhow!("note '{token}' not found on this graph"))
}

fn find(client: &Client, token: &str) -> Option<String> {
    client
        .list_notes()
        .into_iter()
        .find(|n| n.id.starts_with(token) || n.path == token)
        .map(|n| n.id)
}

fn append(client: &Client, id: &str, text: &str) -> Result<()> {
    let body = client.note_body(id)?;
    let next = if body.is_empty() {
        text.to_string()
    } else {
        format!("{body}\n{text}")
    };
    client.edit_note(id, &next)?;
    Ok(())
}

fn render(client: &Client, id: &str) -> Result<()> {
    let body = client.note_body(id)?;
    print!("\x1b[2J\x1b[H{body}\n\n(type a line to append, Ctrl-D to quit)\n> ");
    io::stdout().flush()?;
    Ok(())
}

fn concerns(event: &Event, id: &str) -> bool {
    match event {
        Event::Note { id: changed, .. } => changed == id,
        Event::Notes(_) => true,
        Event::Status(_) => false,
    }
}
