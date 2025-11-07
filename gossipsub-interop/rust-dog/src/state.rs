use libp2p_dog::Transaction;

#[derive(Debug)]
pub(crate) struct State {
    pub transactions_received: Vec<Transaction>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            transactions_received: Vec::new(),
        }
    }
}
