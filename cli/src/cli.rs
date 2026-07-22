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
