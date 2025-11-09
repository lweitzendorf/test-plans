use libp2p::{
    core::ConnectedPoint,
    swarm::{self, SwarmEvent},
};
use slog::{error, info, Logger};

use crate::{
    behaviour::{MyBehaviour, NetworkEvent},
    state::State,
};

async fn handle_dog_event(
    event: libp2p_dog::Event,
    _stderr_logger: Logger,
    _stdout_logger: Logger,
    _swarm: &mut swarm::Swarm<MyBehaviour>,
    state: &mut State,
) {
    match event {
        libp2p_dog::Event::TransactionReceived { transaction, transaction_id, propagation_source } => {
            info!(_stdout_logger, "Received Message";
                "topic" => "-",
                "id" => transaction_id.to_string(),
                "from" => propagation_source.to_string(),
                "size" => transaction.data.len()
            );
            state.transactions_received.push(transaction);
        }
        libp2p_dog::Event::TransactionSent { transaction, transaction_id, propagation_target, .. } => {
            info!(_stdout_logger, "Sent Message";
                "topic" => "-",
                "id" => transaction_id.to_string(),
                "to" => propagation_target.to_string(),
                "size" => transaction.data.len(),
            );
            state.transactions_received.push(transaction);
        }
        libp2p_dog::Event::RoutingUpdated { disabled_routes } => {
            let routes = disabled_routes.into_iter().map(|i| i.to_string()).collect::<String>();
            info!(_stdout_logger, "Updated Routing Table"; "disabled_routes" => routes);
        }
    }
}

async fn handle_swarm_specific_event(
    event: SwarmEvent<NetworkEvent>,
    _stderr_logger: Logger,
    _stdout_logger: Logger,
    _swarm: &mut swarm::Swarm<MyBehaviour>,
    _state: &mut State,
) {
    match event {
        SwarmEvent::NewListenAddr {  .. } => {}
        SwarmEvent::ConnectionEstablished {
            peer_id,
            endpoint,
            ..
        } => match endpoint {
            ConnectedPoint::Dialer { .. } | ConnectedPoint::Listener { .. } => {
                info!(_stdout_logger, "Added Peer"; "id" => peer_id.to_string());
            }
        },
        SwarmEvent::OutgoingConnectionError { error, .. } => {
            error!(_stderr_logger, "Failed to establish connection"; "error" => error.to_string());
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            info!(_stdout_logger, "Removed Peer";
                "id" => peer_id.to_string(),
                "reason" => cause.unwrap().to_string(),
            );
        }
        _ => {}
    }
}

pub(crate) async fn handle_swarm_event(
    event: SwarmEvent<NetworkEvent>,
    stderr_logger: Logger,
    stdout_logger: Logger,
    swarm: &mut swarm::Swarm<MyBehaviour>,
    state: &mut State,

) {
    match event {
        SwarmEvent::Behaviour(NetworkEvent::Dog(event)) => {
            handle_dog_event(event, stderr_logger, stdout_logger, swarm, state).await;
        }
        _ => {
            // Swarm specific events
            handle_swarm_specific_event(event, stderr_logger, stdout_logger, swarm, state).await;
        }
    }
}
