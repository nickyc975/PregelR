pub mod pregel {
    pub mod aggregator;
    pub mod combiner;
    pub mod master;

    mod context;    
    mod message;
    mod state;
    mod vertex;
    mod worker;
}
