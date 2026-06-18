use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

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
mod tests {

    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;

    fn evaluate_expression(sql: &str) -> Option<String> {

        let mut statements = Parser::parse_sql(&MySqlDialect {}, &format!("select {}", sql))
            .expect("expression should parse");

        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected query statement");
        };

        let SetExpr::Select(select) = *query.body else {
            panic!("expected select body");
        };

        let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
            panic!("expected unnamed expression projection");
        };

        let Expr::Function(function) = expression else {
            panic!("expected function projection, got {:?}", expression);
        };

        evaluate_inbuilt_sql_function(function)
            .expect("function should evaluate")
            .map(|value| String::from_utf8(value).expect("result should be utf8"))

    }

    #[test]
    fn distance_returns_zero_for_identical_points() {
        let value = evaluate_expression("distance(12.5, -45.1, 12.5, -45.1)")
            .expect("distance should return a value")
            .parse::<f64>()
            .expect("distance should be numeric");

        assert!(value.abs() < 0.000_001);
    }

    #[test]
    fn distance_returns_expected_meters_for_known_points() {
        let value = evaluate_expression("distance(2.3522, 48.8566, -0.1276, 51.5074)")
            .expect("distance should return a value")
            .parse::<f64>()
            .expect("distance should be numeric");

        // Paris -> London ~= 343.5 km (343,500 meters), allow tolerance.
        assert!((value - 343_500.0).abs() < 6_000.0, "value was {value}");
    }

    #[test]
    fn distance_handles_antimeridian_crossing() {
        let value = evaluate_expression("distance(179.9, 0, -179.9, 0)")
            .expect("distance should return a value")
            .parse::<f64>()
            .expect("distance should be numeric");

        // Around 0.2 degrees on equator ~= 22.24km.
        assert!((value - 22_239.0).abs() < 1_500.0, "value was {value}");
    }

}
