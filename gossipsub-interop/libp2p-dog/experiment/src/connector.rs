use crate::script_instruction::NodeID;
use dns_lookup::lookup_host;
use libp2p::swarm::{dial_opts::DialOpts, DialError, NetworkBehaviour};
use libp2p::{Multiaddr, Swarm};
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("DNS lookup error: {0:?}")]
    DnsLookup(#[from] std::io::Error),
    #[error("Address parse error: {0}")]
    AddrParse(#[from] libp2p::multiaddr::Error),
    #[error("Failed to resolve address: no records returned for `{0}`")]
    FailedToResolveAddress(String),
    #[error("Connection error: {0}")]
    Connect(#[from] DialError),
}

pub async fn connect_to<B: NetworkBehaviour + Send>(
    swarm: &mut Swarm<B>,
    target_node_id: NodeID,
) -> Result<(), ConnectorError> {
    // Resolve IP addresses of the target node
    let hostname = format!("node{target_node_id}");
    let mut ips = lookup_host(&hostname)?;

    // Try to connect using the first IP address
    let ip = ips
        .pop()
        .ok_or(ConnectorError::FailedToResolveAddress(hostname))?;
    let addr = format!("/ip4/{ip}/tcp/9000");
    let multi_addr = Multiaddr::from_str(&addr)?;

    // Attempt to dial the peer
    swarm.dial(
        DialOpts::unknown_peer_id()
            .address(multi_addr.clone())
            .allocate_new_port()
            .build(),
    )?;

    Ok(())
}
