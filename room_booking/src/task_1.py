import sqlite3
import pandas as pd
from room_booking.tables import RoomBooking


def fetch_clashes() -> pd.DataFrame:
    """
    Fetches conflicting bookings from the ROOM_BOOKINGS table and returns a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing conflicting bookings with columns:
                      ROOM_NAME, ORGANIZER, EARLIEST_CLASH_DATE, and CLASH_COUNT.
    """
    room_booking = RoomBooking()
    conn = sqlite3.connect(room_booking.database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            T1.ROOM_NAME,
            T1.ORGANIZER,
            MIN(T2.START_DATE) AS EARLIEST_CLASH_DATE,
            COUNT(*) AS CLASH_COUNT
        FROM
            ROOM_BOOKINGS T1
        INNER JOIN
            ROOM_BOOKINGS T2
        ON
            T1.ROOM_NAME = T2.ROOM_NAME
            AND T1.ORGANIZER != T2.ORGANIZER
            AND T1.START_DATE <= T2.END_DATE
            AND T1.END_DATE >= T2.START_DATE
        GROUP BY
            T1.ROOM_NAME,
            T1.ORGANIZER
        HAVING
            COUNT(*) > 1
        ORDER BY
            EARLIEST_CLASH_DATE ASC;
    """)

    data = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    df_results = pd.DataFrame(data, columns=columns)

    return df_results


def main() -> None:
    """
    Main function to execute and print the conflicting bookings.
    """
    df_results = fetch_clashes()
    print(df_results)


if __name__ == "__main__":
    main()
