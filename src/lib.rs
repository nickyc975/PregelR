pub mod pregel {
    pub mod aggregate;
    pub mod combine;
    pub mod context;
    pub mod master;
    pub mod vertex;

    mod channel;
    mod message;
    mod state;
    mod worker;
}

pub mod examples {
    pub mod page_rank;
    pub mod sssp;
}
