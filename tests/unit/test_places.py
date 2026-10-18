import pytest

from earth_hub.validation.places import Location, parse_place

pytestmark = pytest.mark.req("REQ-DQ-PLACE-001")


@pytest.mark.parametrize(
    ("place", "longitude", "expected"),
    [
        ("86 km E of Ugashik, Alaska", -155.0, ("Ugashik", "Alaska", "United States")),
        ("10 km N of Cobb, CA", -122.7, ("Cobb", "California", "United States")),
        ("3 km N of Lilburn, Georgia", -84.1, ("Lilburn", "Georgia", "United States")),
        ("12 km SSE of Bak’uriani, Georgia", 43.6, ("Bak’uriani", None, "Georgia")),
        ("10 km NE of Mexicali, B.C., MX", -115.3, ("Mexicali", "Baja California", "Mexico")),
        ("8 km SSE of Guarumal, Panama", -82.5, ("Guarumal", None, "Panama")),
        ("3 km WSW of Pāhala, Hawaii", -155.5, ("Pāhala", "Hawaii", "United States")),
        ("15 km E of Utuado, Puerto Rico", -66.5, ("Utuado", None, "Puerto Rico")),
        # Only the first " of " splits: later ones belong to the names.
        (
            "1 km NNE of City of Sammamish, Washington",
            -122.0,
            ("City of Sammamish", "Washington", "United States"),
        ),
        (
            "51 km W of Sake, Democratic Republic of the Congo",
            28.6,
            ("Sake", None, "Democratic Republic of the Congo"),
        ),
        (
            "State of Yap, Federated States of Micronesia",
            138.1,
            ("Yap", None, "Federated States of Micronesia"),
        ),
        # No comma after " of ": an area, not a city.
        ("south of the Fiji Islands", 179.0, (None, "the Fiji Islands", None)),
        ("off the coast of Oregon", -125.5, (None, "Oregon", "United States")),
        # No " of ", nothing to split.
        ("Fiji region", 179.0, (None, None, None)),
        ("Rat Islands, Aleutian Islands, Alaska", 178.0, (None, None, None)),
        ("", 0.0, (None, None, None)),
        (None, 0.0, (None, None, None)),
        ("5 km N of , Alaska", -150.0, (None, None, None)),  # an empty part: not trusted
    ],
)
def test_parse_place(place, longitude, expected):
    assert parse_place(place, longitude) == Location(*expected)


def test_georgia_without_a_longitude_is_the_us_state():
    assert parse_place("4 km N of Washington, Georgia").country == "United States"
