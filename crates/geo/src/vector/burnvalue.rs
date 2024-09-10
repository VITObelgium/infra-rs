#[derive(Debug, Clone)]
pub enum BurnValue<T> {
    Value(T),
    Field(String),
}

impl<T: num::One> Default for BurnValue<T> {
    fn default() -> Self {
        BurnValue::Value(T::one())
    }
}
