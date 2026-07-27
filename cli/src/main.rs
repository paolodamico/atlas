//! atlas-cli: a command-line client over `atlas`.
//!
//! Each vault is a directory. `merge` reconciles two local vaults; `sync`
//! exchanges changes with a relay.
#![allow(clippy::print_stdout, reason = "a CLI prints its results to stdout")]

mod cli;
mod commands;
mod relay_transport;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;

fn main() -> Result<()> {
    commands::run(Cli::parse())
}
