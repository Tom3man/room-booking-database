from typing import Dict
import pandas as pd
from room_booking.common.sqlite_table import SqliteDatabase


class RoomBooking(SqliteDatabase):

    DEFAULT_PATH: str = "ROOM_BOOKINGS"
    DEFAULT_SCHEMA: Dict[str, str] = {
        "ROOM_NAME": "VARCHAR(30)",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "ORGANIZER": "VARCHAR(30)",
        "NOTES": "VARCHAR(30)",
        "LOAD_DATE": "DATE",
    }

    def format_ingested_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method to format the ingested DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame to be formatted.

        Returns:
            pd.DataFrame: The formatted DataFrame.
        """

        # Remove non-ASCII characters from the 'col1' column using regex
        df['room_name'] = df['room_name'].str.replace(
            r'[^\x00-\x7F]+', '', regex=True)

        # Swap start_date and end_date where start_date > end_date
        mask = df['start_date'] > df['end_date']
        df.loc[mask, ['start_date', 'end_date']] = df.loc[
            mask, ['end_date', 'start_date']].values

        return df
