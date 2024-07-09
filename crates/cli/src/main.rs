use clap::Parser;
use console::style;
use std::process;

mod cmd;
mod logging;

type StdError = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() {
    let cmd = cmd::Cmd::parse();

    let _guard = logging::init_logger(cmd.pretty.unwrap_or(false), cmd.log_level.unwrap_or(1))
        .unwrap_or_else(|e| print_error(e));

    let res = match cmd.command {
        cmd::Commands::Send(args) => args.send().await,
        cmd::Commands::Convert(args) => args.convert(),
    };

    res.unwrap_or_else(|e| print_error(e));
}

fn print_error(e: StdError) -> ! {
    eprintln!("{} {e}", style("error:").red().bright().bold());
    process::exit(1)
}
