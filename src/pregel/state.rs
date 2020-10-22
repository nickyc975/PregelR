/**
 * Global state.
 *
 * INITIALIZED ---> LOADED ---> CLEANED ---> COMPUTED
 *                                 ^            |
 *                                 |            |
 *                                  ------------
 */
pub enum State {
    INITIALIZED, // the master is just created.
    LOADED,      // workers loaded data.
    CLEANED,     // workers did clean up before compute.
    COMPUTED,    // workers finished one superstep.
}
