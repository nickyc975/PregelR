pub trait Aggregate<V> {
    fn clone(&self) -> dyn Aggregate<V>;

    fn agg_vertex(&self, a: &V, b: &V);

    fn agg_value_of(&self, a: &dyn Aggregate<V>, b: &dyn Aggregate<V>);

    fn clear(&self);
}
