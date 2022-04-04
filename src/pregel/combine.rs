pub trait Combine<M>: Send + Sync {
    fn combine(&self, a: M, b: M) -> M;
}
