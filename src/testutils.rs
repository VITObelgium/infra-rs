pub const NOD: f64 = 255.0;

pub fn create_vec<T: num::NumCast>(data: &[f64]) -> Vec<T> {
    data.iter().map(|&v| num::NumCast::from(v).unwrap()).collect()
}

pub fn to_f64<T: num::ToPrimitive + Copy>(data: Vec<Option<T>>) -> Vec<Option<f64>> {
    data.iter().map(|&v| v.and_then(|v| v.to_f64())).collect()
}
