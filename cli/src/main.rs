//! atlas-cli: a command-line client over `atlas`
//!
//! Each vault is a directory; `sync` reconciles one vault with another.
#![allow(clippy::print_stdout, reason = "a CLI prints its results to stdout")]

mod cli;
mod commands;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;

fn main() -> Result<()> {
    commands::run(Cli::parse())
}
