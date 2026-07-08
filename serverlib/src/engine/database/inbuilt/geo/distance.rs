use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::super::numeric::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct DistanceCommand;

fn normalize_longitude_delta_degrees(delta: f64) -> f64 {
    // Fold into [-180, 180) so anti-meridian crossings take the shorter arc.
    (delta + 180.0).rem_euclid(360.0) - 180.0
}

// returns the distance between two points in meters given 2 points in the format, long, lat

impl InbuiltServerCommand for DistanceCommand {

    fn name(&self) -> &'static str {
        "DISTANCE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 4, 4, self.name())?;

        let Some(lon1) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };

        let Some(lat1) = evaluate_f64_arg(args, 1)? else {
            return Ok(None);
        };

        let Some(lon2) = evaluate_f64_arg(args, 2)? else {
            return Ok(None);
        };

        let Some(lat2) = evaluate_f64_arg(args, 3)? else {
            return Ok(None);
        };

        // Great-circle distance in meters.
        let earth_radius_m = 6_371_000.0_f64;

        let lat1_rad = lat1.to_radians();
        let lat2_rad = lat2.to_radians();
        let delta_lat_rad = (lat2 - lat1).to_radians();
        let delta_lon_rad = normalize_longitude_delta_degrees(lon2 - lon1).to_radians();

        let sin_half_dlat = (delta_lat_rad / 2.0).sin();
        let sin_half_dlon = (delta_lon_rad / 2.0).sin();

        let a = (sin_half_dlat * sin_half_dlat
            + lat1_rad.cos() * lat2_rad.cos() * sin_half_dlon * sin_half_dlon)
            .clamp(0.0, 1.0);

        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        Ok(float_result(earth_radius_m * c))
        
    }

}

#[cfg(test)]
#[path = "distance_test.rs"]
mod tests;
