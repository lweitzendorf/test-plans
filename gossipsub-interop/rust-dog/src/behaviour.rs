use libp2p::{identity::Keypair};

#[derive(Debug)]
pub(crate) enum NetworkEvent {
    Dog(libp2p_dog::Event),
}

impl From<libp2p_dog::Event> for NetworkEvent {
    fn from(event: libp2p_dog::Event) -> Self {
        Self::Dog(event)
    }
}

#[derive(libp2p_swarm::NetworkBehaviour)]
#[behaviour(prelude = "libp2p_swarm::derive_prelude", to_swarm = "NetworkEvent")]
pub(crate) struct MyBehaviour {
    pub dog: libp2p_dog::Behaviour,
}

impl MyBehaviour {
    pub(crate) fn new(key: &Keypair) -> Self {
        let dog = libp2p_dog::Behaviour::new(
            libp2p_dog::TransactionAuthenticity::Signed(key.clone()),
            libp2p_dog::Config::default(),
        )
        .expect("Failed to create dog behaviour");

        Self { dog }
    }
}
