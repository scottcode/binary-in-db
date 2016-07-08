import os
import psycopg2
from psycopg2.extensions import AsIs


def create_table_for_binary(conn, schema="test", table="rawfiles", force=False):
    sql_drop = "drop table if exists %s.%s;"
    creation_sql = """
        create table %s.%s (
            filename text,
            contents bytea
        );
    """
    with conn.cursor() as cur:
        if force:
            cur.execute(sql_drop, (AsIs(schema), AsIs(table)))
        cur.execute(creation_sql, (AsIs(schema), AsIs(table)))


def load_raw_file(fpath, conn, schema="test", table="rawfiles"):
    """Load binary contents of fpath into `schema`.`table`"""
    with open(fpath, 'rb') as f:
        data = f.read()
    sql = "insert into %s.%s (filename, contents) values (%s, %s);"
    with conn.cursor() as cur:
        cur.execute(sql, (AsIs(schema), AsIs(table), fpath, psycopg2.Binary(data)))


def load_folder(path, conn, schema="test", table="rawfiles"):
    """Load all files in immediate folder specified by path
    Returns a dictionary specifying which files succeeded and which failed"""
    files = os.listdir(path)
    result = {
        'succeeded': [],
        'failed': []
    }
    for fname in files:
        fpath = os.path.join(path, fname)
        if os.path.isfile(fpath) and not fname.startswith('.'):
            if not os.access(fpath, os.R_OK):
                result['failed'].append((fpath, "access error"))
            try:
                load_raw_file(fpath, conn, schema=schema, table=table)
                conn.commit()
                result['succeeded'].append(fpath)
            except Exception as e:
                conn.rollback()
                result['failed'].append((fpath, 'load error: {}'.format(e)))
    return result
