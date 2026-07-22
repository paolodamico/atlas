//! Subcommand handlers.

use std::io::{self, IsTerminal, Read};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use atlas_core::{FileStore, Vault, VaultError};

use crate::cli::{Cli, Command};

/// Runs the parsed CLI.
///
/// # Errors
/// Propagates any command failure.
pub fn run(cli: Cli) -> Result<()> {
    let Cli { vault, command } = cli;
    let dir = vault.as_path();
    match command {
        Command::Init => init(dir),
        Command::Add { path, title, body } => add(dir, &path, title.as_deref(), body),
        Command::List => list(dir),
        Command::Show { note } => show(dir, &note),
        Command::Edit { note, body } => edit(dir, &note, body),
        Command::Rm { note } => remove(dir, &note),
        Command::Sync { other } => sync(dir, &other),
    }
}

fn init(dir: &Path) -> Result<()> {
    let store =
        FileStore::new(dir).with_context(|| format!("creating vault at {}", dir.display()))?;
    match Vault::load_existing(store.clone()) {
        Ok(_) => bail!("vault already initialized at {}", dir.display()),
        Err(VaultError::NoRootDoc) => {}
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context(format!("cannot read existing vault at {}", dir.display())));
        }
    }
    let mut vault = Vault::new(store)?;
    vault.persist()?;
    println!("initialized vault at {}", dir.display());
    Ok(())
}

fn add(dir: &Path, path: &str, title: Option<&str>, body: Option<String>) -> Result<()> {
    let mut vault = open(dir)?;
    let body = read_body(body)?;
    let (id, _) = vault.create_note(path, title.unwrap_or(path), &body)?;
    println!("added {} ({path})", short(&id));
    Ok(())
}

fn list(dir: &Path) -> Result<()> {
    let vault = open(dir)?;
    let notes = vault.list_notes(0, 10_000);
    if notes.is_empty() {
        println!("(no notes)");
        return Ok(());
    }
    for n in notes {
        println!("{}  {}  [{}]", short(&n.id), n.path, n.title);
    }
    Ok(())
}

fn show(dir: &Path, note: &str) -> Result<()> {
    let vault = open(dir)?;
    let id = resolve(&vault, note)?;
    println!("{}", vault.get_note(&id)?.body()?);
    Ok(())
}

fn edit(dir: &Path, note: &str, body: Option<String>) -> Result<()> {
    let mut vault = open(dir)?;
    let id = resolve(&vault, note)?;
    let body = read_body(body)?;
    let mut doc = vault.get_note(&id)?;
    doc.set_body(&body)?;
    vault.update_note(&id, &mut doc)?;
    println!("edited {}", short(&id));
    Ok(())
}

fn remove(dir: &Path, note: &str) -> Result<()> {
    let mut vault = open(dir)?;
    let id = resolve(&vault, note)?;
    vault.delete_note(&id)?;
    println!("deleted {}", short(&id));
    Ok(())
}

fn sync(dir: &Path, other_dir: &Path) -> Result<()> {
    let mut vault = open(dir)?;
    let other = open(other_dir)?;
    let report = vault.merge_from(&other)?;
    println!(
        "pulled from {}: {} merged, {} new",
        other_dir.display(),
        report.merged,
        report.pulled
    );
    Ok(())
}

/// Opens an existing vault, with a friendly error if it isn't initialized.
fn open(dir: &Path) -> Result<Vault> {
    let store =
        FileStore::new(dir).with_context(|| format!("opening vault at {}", dir.display()))?;
    match Vault::load_existing(store) {
        Ok(vault) => Ok(vault),
        Err(VaultError::NoRootDoc) => Err(anyhow!(
            "no vault at {} (run `atlas init` first)",
            dir.display()
        )),
        Err(e) => Err(anyhow::Error::new(e).context(format!("opening vault at {}", dir.display()))),
    }
}

/// Resolves a token (note id, unique id prefix, or exact path) to one id.
fn resolve(vault: &Vault, token: &str) -> Result<String> {
    let matches: Vec<String> = vault
        .list_notes(0, 100_000)
        .into_iter()
        .filter(|n| n.id.starts_with(token) || n.path == token)
        .map(|n| n.id)
        .collect();
    match matches.as_slice() {
        [only] => Ok(only.clone()),
        [] => bail!("no note matching '{token}'"),
        many => bail!("'{token}' is ambiguous ({} matches)", many.len()),
    }
}

/// Returns `body` if given, else reads stdin (erroring on an interactive tty).
fn read_body(body: Option<String>) -> Result<String> {
    if let Some(body) = body {
        return Ok(body);
    }
    if io::stdin().is_terminal() {
        bail!("no body given: pass --body or pipe content on stdin");
    }
    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf)?;
    Ok(buf)
}

/// A short, copy-pasteable form of a note id for display.
fn short(id: &str) -> String {
    id.chars().take(12).collect()
}
