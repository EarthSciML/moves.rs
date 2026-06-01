//! Error type for `moves-onroad-retrofit`.

use thiserror::Error;

/// Errors returned by the OnRoadRetrofit control strategy.
#[derive(Debug, Error)]
pub enum Error {
 /// A `cumulativeRetrofitFraction` value is outside `[0.0, 1.0]`.
    #[error(
        "retrofit record (sourceType={source_type_id}, modelYear={start_model_year}..={end_model_year}, \
         retrofitYear={retrofit_year_id}, pollutant={pollutant_id}, process={process_id}) \
         has cumulativeRetrofitFraction={fraction} outside [0.0, 1.0]"
    )]
    FractionOutOfRange {
        source_type_id: i32,
        start_model_year: i32,
        end_model_year: i32,
        retrofit_year_id: i32,
        pollutant_id: u16,
        process_id: u16,
        fraction: f64,
    },

 /// A `retrofitEffectiveness` value is outside `[0.0, 1.0]`.
    #[error(
        "retrofit record (sourceType={source_type_id}, modelYear={start_model_year}..={end_model_year}, \
         retrofitYear={retrofit_year_id}, pollutant={pollutant_id}, process={process_id}) \
         has retrofitEffectiveness={effectiveness} outside [0.0, 1.0]"
    )]
    EffectivenessOutOfRange {
        source_type_id: i32,
        start_model_year: i32,
        end_model_year: i32,
        retrofit_year_id: i32,
        pollutant_id: u16,
        process_id: u16,
        effectiveness: f64,
    },
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, Error>;
