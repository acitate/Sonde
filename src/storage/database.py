import sqlite3
from pathlib import Path


def create_database(path: str | Path) -> sqlite3.Connection:
    """Create the SQLite database and parent directories if necessary.

    Args:
        path (str | Path): Path to the database file.

    Returns:
        sqlite3.Connection: Connection to the created database.
    """
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return sqlite3.connect(db_path)
