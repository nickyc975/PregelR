pub trait Combine<M> {
    fn combine(&self, a: &M, b: &M) -> M;
}