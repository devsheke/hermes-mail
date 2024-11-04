use clap::Parser;
use console::style;
use hermes_logger::init_logger;
use std::process;

mod cmd;

type StdError = Box<dyn std::error::Error>;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cmd = cmd::Cmd::parse();

    let _guard = init_logger(cmd.pretty.unwrap_or(false), cmd.log_level.unwrap_or(1))
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
