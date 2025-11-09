use clap::Parser;
use slog::{o, Drain, FnValue, Logger, PushFnValue, Record};
use std::time::Instant;

mod behaviour;
mod handler;
mod state;
mod swarm;

mod connector;
mod experiment;
mod script_instruction;

use experiment::run_experiment;
use script_instruction::{ExperimentParams, NodeID};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Path to the params file
    #[clap(long, value_name = "FILE")]
    params: String,
}

fn create_logger() -> (Logger, Logger) {
    // Create stderr logger for most messages
    let stderr_drain = slog_json::Json::new(std::io::stderr())
        .add_key_value(o!(
            "time" => FnValue(move |_ : &Record| {
                    time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .ok()
            }),
            "level" => FnValue(move |rinfo : &Record| {
                rinfo.level().as_short_str()
            }),
            "msg" => PushFnValue(move |record : &Record, ser| {
                ser.emit(record.msg())
            }),
        ))
        .build()
        .fuse();
    let stderr_drain = slog_async::Async::new(stderr_drain).build().fuse();
    let stderr_logger = slog::Logger::root(stderr_drain, o!());

    // Create stdout logger for special messages
    let stdout_drain = slog_json::Json::new(std::io::stdout())
        .add_key_value(o!(
            "time" => FnValue(move |_ : &Record| {
                    time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .ok()
            }),
            "level" => FnValue(move |rinfo : &Record| {
                rinfo.level().as_short_str()
            }),
            "msg" => PushFnValue(move |record : &Record, ser| {
                ser.emit(record.msg())
            }),
        ))
        .build()
        .fuse();
    let stdout_drain = slog_async::Async::new(stdout_drain).build().fuse();
    let stdout_logger = Logger::root(stdout_drain, o!());

    (stderr_logger, stdout_logger)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let params = ExperimentParams::from_json_file(&args.params)?;

    let start_time = Instant::now();
    let node_id = NodeID::new()?;

    let (stderr_logger, stdout_logger) = create_logger();

    let mut swarm = swarm::new_swarm();
    swarm.listen_on("/ip4/0.0.0.0/tcp/9000".parse()?)?;

    run_experiment(
        start_time,
        stderr_logger,
        stdout_logger,
        swarm,
        node_id,
        params,
    )
    .await?;

    Ok(())
}
