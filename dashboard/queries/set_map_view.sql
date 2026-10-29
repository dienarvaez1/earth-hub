-- Parameters, in order: strong_magnitude, cell_degrees, west, south, east, north, area_limit.
UPDATE map_view SET
    strong_magnitude = ?::DOUBLE,
    cell_degrees = ?::DOUBLE,
    west = ?::DOUBLE,
    south = ?::DOUBLE,
    east = ?::DOUBLE,
    north = ?::DOUBLE,
    area_limit = ?::INTEGER;
