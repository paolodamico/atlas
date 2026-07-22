//! Command-line interface definition.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Command-line interface for `atlas`.
///
/// Each vault is a directory; `sync` reconciles one vault with another.
#[derive(Parser)]
#[command(name = "atlas", version)]
pub struct Cli {
    /// Vault directory to operate on.
    #[arg(long, default_value = "atlas-vault", global = true)]
    pub vault: PathBuf,

    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// A top-level command.
#[derive(Subcommand)]
pub enum Command {
    /// Create a new, empty vault.
    Init,
    /// Add a note (body from --body, otherwise read from stdin).
    Add {
        /// Path of the note within the vault.
        path: String,
        /// Display title (defaults to the path).
        #[arg(short, long)]
        title: Option<String>,
        /// Note body. If omitted, read from stdin.
        #[arg(short, long)]
        body: Option<String>,
    },
    /// List all notes in the vault.
    List,
    /// Print a note's body.
    Show {
        /// Note id, unique id prefix, or exact path.
        note: String,
    },
    /// Replace a note's body (body from --body, otherwise read from stdin).
    Edit {
        /// Note id, unique id prefix, or exact path.
        note: String,
        /// New body. If omitted, read from stdin.
        #[arg(short, long)]
        body: Option<String>,
    },
    /// Delete a note.
    Rm {
        /// Note id, unique id prefix, or exact path.
        note: String,
    },
    /// Pull another vault's changes into this one.
    Sync {
        /// The other vault's directory.
        other: PathBuf,
    },
}
