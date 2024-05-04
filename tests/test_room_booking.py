import os
import pandas as pd
import pytest
from room_booking.tables import RoomBooking


@pytest.fixture
def sample_data():
    """
    Fixture providing sample data for testing.
    """
    data = {
        "ROOM_NAME": ["Room1", "Room2", "Room3"],
        "START_DATE": ["2024-01-01", "2024-02-01", "2024-03-01"],
        "END_DATE": ["2024-01-05", "2024-02-05", "2024-03-05"],
        "ORGANIZER": ["Organizer1", "Organizer2", "Organizer3"],
        "NOTES": ["Note1", "Note2", "Note3"],
        "LOAD_DATE": ["2024-04-01", "2024-04-02", "2024-04-03"],
    }
    return pd.DataFrame(data)


def test_default_schema_matches_database():
    """
    Test if the DEFAULT_SCHEMA matches the schema of the actual database table.
    """
    room_booking = RoomBooking()

    expected_schema = room_booking.DEFAULT_SCHEMA
    actual_schema = {
        "ROOM_NAME": "VARCHAR(30)",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "ORGANIZER": "VARCHAR(30)",
        "NOTES": "VARCHAR(30)",
        "LOAD_DATE": "DATE",
    }

    assert expected_schema == actual_schema


def test_database_file_exists(sample_data):
    """
    Test if the database file exists.
    """
    room_booking = RoomBooking()
    assert os.path.exists(room_booking.database)
