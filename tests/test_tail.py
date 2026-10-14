from streaming.tail import HEADER, format_reading


def reading(**over):
    rec = {
        "batch_id": 94, "seq": 7, "time_h": 1.4, "event_time": 1_672_531_200_000,
        "temperature_k": 298.15, "ph": 6.5, "dissolved_oxygen_mgpl": 14.9, "agitator_rpm": 100.0,
        "aeration_rate_lph": 60.0, "penicillin_conc_gpl": None, "fault_ref": 0,
    }
    rec.update(over)
    return rec


def test_format_normal_reading():
    line = format_reading(reading())
    assert line.startswith("2023-01-01 00:00:00    94     7    1.4")
    assert "298.15" in line and "FAULT" not in line


def test_missing_values_show_a_dash_and_columns_line_up():
    line = format_reading(reading())
    assert line.rstrip().endswith("-")
    assert len(line) == len(HEADER)


def test_fault_readings_are_flagged():
    assert format_reading(reading(fault_ref=2)).endswith("<< FAULT 2")
