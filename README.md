# room-booking-database

# Room Booking Database

## Installation 

This repository is poetry-enabled. To get started, run the following commands:

```bash
poetry lock
poetry install --no-cache
```

## Usage

### Building a Database
To build the database from a CSV file, navigate to the room_booking/src folder and run the create_database.py script, passing in the location of the CSV (or XLSM) file as a command-line parameter. This will create a .db file in the databases directory.

To run the script with poetry, use the following command:

```python
poetry run python create_database.py <path-to-csv-file>
```

### Running the Task Queries

Each of the task queries relies on the database being built, so we recommend running the above step beforehand. Each script will print the output of the SQL queries to the terminal. They can be run as follows:

```python
poetry run python task_a.py
```
or

```python
poetry run python task_b.py
```
