//! Array operations with nodata handling.

pub trait AddInclusive<Rhs = Self> {
    /// The resulting type after applying the `+` operator.
    type Output;

    /// Performs the `+` operation
    /// if only one of the operands is nodata the result is the value of the non-nodata operand.
    #[must_use = "this returns the result of the operation, without modifying the original"]
    fn add_inclusive(self, rhs: Rhs) -> Self::Output;
}

pub trait SubInclusive<Rhs = Self> {
    /// The resulting type after applying the `-` operator.
    type Output;

    /// Performs the `-` operation
    /// if only one of the operands is nodata the result is the value of the non-nodata operand.
    #[must_use = "this returns the result of the operation, without modifying the original"]
    fn sub_inclusive(self, rhs: Rhs) -> Self::Output;
}

pub trait AddAssignInclusive<Rhs = Self> {
    /// Performs the `+=` operation.
    /// if only one of the operands is nodata the result is the value of the non-nodata operand.
    fn add_assign_inclusive(&mut self, rhs: Rhs);
}

pub trait SubAssignInclusive<Rhs = Self> {
    /// Performs the `-=` operation.
    /// if only one of the operands is nodata the result is the value of the non-nodata operand.
    fn sub_assign_inclusive(&mut self, rhs: Rhs);
}
