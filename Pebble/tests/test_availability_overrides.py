def test_parse_availability_overrides_accepts_sheets_dates():
    from pebble.cli import parse_availability_overrides

    rows = [
        ["Night", "Main", "Avail Pre?", "Avail Post?", "Reason"],
        ["7/2/24", "Alice", "y", "n", ""],
        ["2024-07-03", "Bob", "yes", "", ""],
        ["July 4, 2024", "Charlie", "", "t", ""],
    ]
    roster_map = {"Charlie": "CharlieMain"}
    overrides = parse_availability_overrides(rows, roster_map)
    assert overrides == {
        "2024-07-02": {"Alice": {"pre": True, "post": False}},
        "2024-07-03": {"Bob": {"pre": True, "post": None}},
        "2024-07-04": {"CharlieMain": {"pre": None, "post": True}},
    }
