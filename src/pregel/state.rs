/**
 * Global state.
 *
 * INITIALIZED ---> LOADED ---> CLEANED ---> COMPUTED ---> COMMUNICATED
 *                                 ^                            |
 *                                 |                            |
 *                                  ----------------------------
 */
#[derive(Debug)]
pub enum State {
    INITIALIZED,  // the master is just created.
    LOADED,       // workers loaded data.
    CLEANED,      // workers did clean up before compute.
    COMPUTED,     // workers finished computing.
    COMMUNICATED, // works finished one superstep.
}
