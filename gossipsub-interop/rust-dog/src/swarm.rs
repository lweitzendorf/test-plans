use crate::behaviour::MyBehaviour;
use libp2p::{noise, tcp, yamux, SwarmBuilder};
use std::time::Duration;

pub(crate) fn new_swarm() -> libp2p::Swarm<MyBehaviour> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .unwrap()
        .with_behaviour(|key| MyBehaviour::new(key))
        .unwrap()
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX)))
        .build()
}
