import click
from room_booking.tables import RoomBooking


@click.command()
@click.option('--file-path', '-f', type=str, help='Path to the CSV file.')
def main(file_path):

    if not file_path:
        click.echo("Please provide a file path.")
        return

    room_booking = RoomBooking()
    room_booking.create_table()

    room_booking.create_database_from_file(file_path=file_path)


if __name__ == "__main__":
    main()
