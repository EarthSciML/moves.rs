//! Calculator and Generator traits plus the registry that wires them
//! into the master loop.

pub mod registry;
pub mod traits;

pub use registry::{CalculatorFactory, CalculatorRegistry, GeneratorFactory, ModuleFactory};
pub use traits::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Generator,
};
