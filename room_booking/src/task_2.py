import sqlite3
import pandas as pd
from room_booking.tables import RoomBooking


def fetch_reschedules() -> pd.DataFrame:
    """
    Fetches bookings that need rescheduling from the ROOM_BOOKINGS table and returns a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing conflicting bookings with columns:
                      ROOM_NAME, START_DATE_FIRST, END_DATE_FIRST,
                      ORGANIZER_FIRST, START_DATE_CLASH, END_DATE_CLASH,
                      and ORGANIZER_CLASH.
    """
    room_booking = RoomBooking()
    conn = sqlite3.connect(room_booking.database)
    cursor = conn.cursor()

    cursor.execute("""
        WITH Clashes AS (
            SELECT
                F1.ROOM_NAME,
                F1.START_DATE AS START_DATE_FIRST,
                F1.END_DATE AS END_DATE_FIRST,
                F1.ORGANIZER AS ORGANIZER_FIRST,
                F2.START_DATE AS START_DATE_CLASH,
                F2.END_DATE AS END_DATE_CLASH,
                F2.ORGANIZER AS ORGANIZER_CLASH
            FROM
                ROOM_BOOKINGS F1
            INNER JOIN
                ROOM_BOOKINGS F2 ON F1.ROOM_NAME = F2.ROOM_NAME
                                AND F1.START_DATE < F2.END_DATE
                                AND F1.END_DATE > F2.START_DATE
                                AND F2.START_DATE > F1.START_DATE
                                AND F1.ORGANIZER != F2.ORGANIZER
        ),
        ClashCounts AS (
            SELECT
                T1.ROOM_NAME,
                T1.ORGANIZER,
                MIN(T1.START_DATE) AS EARLIEST_CLASH_DATE,
                COUNT(*) AS CLASH_COUNT
            FROM
                ROOM_BOOKINGS T1
            INNER JOIN
                ROOM_BOOKINGS T2 ON T1.ROOM_NAME = T2.ROOM_NAME
                                AND T1.ORGANIZER != T2.ORGANIZER
                                AND T1.START_DATE <= T2.END_DATE
                                AND T1.END_DATE >= T2.START_DATE
            GROUP BY
                T1.ROOM_NAME,
                T1.ORGANIZER
            HAVING
                COUNT(*) > 1
        )
        SELECT
            Clashes.ROOM_NAME,
            Clashes.START_DATE_FIRST,
            Clashes.END_DATE_FIRST,
            Clashes.ORGANIZER_FIRST,
            Clashes.START_DATE_CLASH,
            Clashes.END_DATE_CLASH,
            Clashes.ORGANIZER_CLASH
        FROM
            Clashes
        INNER JOIN
            ClashCounts ON Clashes.ROOM_NAME = ClashCounts.ROOM_NAME
                    AND Clashes.START_DATE_FIRST = ClashCounts.EARLIEST_CLASH_DATE
        ORDER BY
            Clashes.ORGANIZER_FIRST,
            Clashes.START_DATE_FIRST;
    """)

    data = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    df_results = pd.DataFrame(data, columns=columns)

    return df_results


def main() -> None:
    """
    Main function to execute and print the conflicting bookings.
    """
    df_results = fetch_reschedules()
    print(df_results)


if __name__ == "__main__":
    main()