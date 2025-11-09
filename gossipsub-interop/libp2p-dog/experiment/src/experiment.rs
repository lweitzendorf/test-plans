use byteorder::{BigEndian, ByteOrder};
use futures::{StreamExt};
use libp2p::{Swarm};
use slog::{error, info, Logger};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use crate::behaviour::MyBehaviour;
use crate::connector;
use crate::script_instruction::{ExperimentParams, NodeID, ScriptInstruction};
use crate::handler;
use crate::state::State;

pub struct ScriptedNode {
    node_id: NodeID,
    swarm: Swarm<MyBehaviour>,
    state: State,
    stderr_logger: Logger,
    stdout_logger: Logger,
    start_time: Instant,
}

impl ScriptedNode {
    pub fn new(
        node_id: NodeID,
        swarm: Swarm<MyBehaviour>,
        stderr_logger: Logger,
        stdout_logger: Logger,
        start_time: Instant,
    ) -> Self {
        info!(stdout_logger, "PeerID"; "id" => %swarm.local_peer_id(), "node_id" => %node_id);
        let state = State::new();
        Self {
            node_id,
            swarm,
            state,
            stderr_logger,
            stdout_logger,
            start_time,
        }
    }

    pub async fn run_instruction(
        &mut self,
        instruction: ScriptInstruction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match instruction {
            ScriptInstruction::Connect { connect_to } => {
                for target_node_id in connect_to {
                    match connector::connect_to(&mut self.swarm, target_node_id).await {
                        Ok(_) => {
                            info!(self.stderr_logger, "Connected to node {}", target_node_id);
                        }
                        Err(e) => {
                            error!(
                                self.stderr_logger,
                                "Failed to connect to node {}: {}", target_node_id, e
                            );
                            return Err(e.into());
                        }
                    }
                }
                info!(
                    self.stderr_logger,
                    "Node {} connected to peers", self.node_id
                );
            }
            ScriptInstruction::IfNodeIDEquals {
                node_id,
                instruction,
            } => {
                if node_id == self.node_id {
                    Box::pin(self.run_instruction(*instruction)).await?;
                }
            }
            ScriptInstruction::WaitUntil { elapsed_seconds } => {
                let target_time = self.start_time + Duration::from_secs(elapsed_seconds);
                let now = Instant::now();

                if now < target_time {
                    let wait_time = target_time.duration_since(now);
                    info!(
                        self.stderr_logger,
                        "Waiting {:?} (until elapsed: {}s)", wait_time, elapsed_seconds
                    );

                    // Create a timeout future
                    let mut timeout = Box::pin(sleep(wait_time));

                    // Process events while waiting for the timeout
                    loop {
                        tokio::select! {
                            _ = &mut timeout => {
                                // Timeout complete, we can continue
                                break;
                            }
                            event = self.swarm.select_next_some() => {
                                // Process any messages that arrive during sleep
                                handler::handle_swarm_event(
                                    event, self.stderr_logger.clone(), self.stdout_logger.clone(),
                                    &mut self.swarm, &mut self.state
                                ).await;
                            }
                        }
                    }
                }
            }
            ScriptInstruction::Publish {
                message_id,
                message_size_bytes,
                topic_id: _,
            } => {
                let mut msg = vec![0u8; message_size_bytes];
                BigEndian::write_u64(&mut msg, message_id);

                match self
                    .swarm
                    .behaviour_mut()
                    .dog
                    .publish(msg.clone())
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            self.stderr_logger,
                            "Failed to publish message";
                            "id" => message_id,
                            "error" => ?e
                        );
                        return Err(Box::new(std::io::Error::other(e.to_string())));
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn run_experiment(
    start_time: Instant,
    stderr_logger: Logger,
    stdout_logger: Logger,
    swarm: Swarm<MyBehaviour>,
    node_id: NodeID,
    params: ExperimentParams,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut node = ScriptedNode::new(
        node_id,
        swarm,
        stderr_logger.clone(),
        stdout_logger.clone(),
        start_time,
    );
    for instruction in params.script {
        node.run_instruction(instruction).await?;
    }
    Ok(())
}

