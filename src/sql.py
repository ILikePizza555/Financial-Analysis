import apsw
import apsw.ext
import logging

from typing import Sequence

LOGGER = logging.getLogger("sql.csv_loader")

def upsert_many_transactions(conn: apsw.Connection, records: Sequence[dict]) -> list[int]:
    """
    Inserts all transactions into the db, ignoring if they already exist.
    Returns a list of the upserted transaction ids in the db.
    """
    
    sql = """
        INSERT INTO transactions(Description, PostDate, Amount, Source)
        VALUES (:Description, :Date, :Amount, :Source)
        ON CONFLICT(Description, PostDate, Amount, Source) DO NOTHING
        RETURNING id
    """

    cursor = conn.cursor()
    cursor.executemany(sql, records)
    return [columns[0] for columns in cursor]

def upsert_many_tags(conn: apsw.Connection, tags: Sequence[str]) -> dict[str, int]:
    """
    Inserts all tags into the db, ignoring if they already exist.
    Assumes iteration remains consistent if the sequence isn't modified.
    Returns a dict mapping each tag to it's id in the db. This dict may be smaller than the sequence passed in due to duplicates. 
    """

    sql = """
        INSERT INTO tags(name)
        VALUES (?)
        ON CONFLICT(name) DO NOTHING
        RETURNING id
    """

    cursor = conn.cursor()
    cursor.executemany(sql, ((t,) for t in tags))    
    return {t: columns[0] for t, columns in zip(tags, cursor)}

def insert_transaction_tag_relations(conn: apsw.Connection, transaction_ids: Sequence[int], transactions: list[dict], tag_ids: dict[str, int]):    
    transaction_tag_relation = (
        (transaction_id, tag_ids[tag]) for transaction_id, record in zip(transaction_ids, transactions) 
                                       for tag in record["Tags"])
    
    sql = """INSERT INTO transaction_tags(transaction_id, tag_id) VALUES (?, ?)"""

    cursor = conn.cursor()
    cursor.executemany(sql, transaction_tag_relation)
    return cursor