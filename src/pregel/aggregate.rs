pub trait Aggregate<V>: Clone {
    fn agg_vertex(&self, a: &V, b: &V);

    fn agg_value_of(&self, a: &Self, b: &Self);

    fn clear(&self);
}