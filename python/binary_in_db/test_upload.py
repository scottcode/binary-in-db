from unittest import TestCase
import os
from pprint import pformat
import psycopg2
from .upload import load_raw_file, load_folder, create_table_for_binary

TEST_DATA_DIR = r'../../test_data'
CONNECTION_INFO = {
    'host': 'localhost',
    'port': '5432',
    'database': 'shajek',
    'user': 'postgres',
    'password': ''
}

table_definition = """
        -- Create table to hold raw binary contents of files
        drop table if exists test.rawfiles;
        create table test.rawfiles (
            filename text,
            contents bytea
        )
        --DISTRIBUTED RANDOMLY
        ;
    """
ignore_files = {'.DS_Store'}

def connect(host, port, database, user, password):
    #Initialize connection string
    conn_str =  """dbname='{database}' user='{user}' host='{host}' port='{port}' password='{password}'""".format(
                    database=database,
                    host=host,
                    port=port,
                    user=user,
                    password=password
            )
    return psycopg2.connect(conn_str)


class UploadFromLocal(TestCase):
    conn = None

    def setUp(self):
        # establish db connection
        self.conn = connect(**CONNECTION_INFO)
        create_table_for_binary(self.conn, force=True)
        self.conn.commit()
        # with self.conn.cursor() as cur:
        #     cur.execute(table_definition)
        #     self.conn.commit()
        self.files = tuple(set(os.listdir(TEST_DATA_DIR)) - ignore_files)

    def tearDown(self):
        self.conn.close()

    def test_load_1_file(self):
        load_raw_file(
            os.path.join(TEST_DATA_DIR, self.files[0]),  # path
            self.conn
        )
        with self.conn.cursor() as cur:
            cur.execute('select filename, length(contents) from test.rawfiles')
            result = cur.fetchall()
        self.assertEqual(len(result), 1, "Loading individual file failed. Expected 1 loaded, got {0}".format(len(result)))
        names, lengths = zip(*result)
        self.assertGreater(min(lengths), 0, "Some files got zero bytes uploaded: {0}".format(lengths))

    def test_load_folder(self):
        load_result = load_folder(TEST_DATA_DIR, self.conn)
        self.assertFalse(load_result['failed'], "Upload failed on: {0}".format(load_result['failed']))
        with self.conn.cursor() as cur:
            cur.execute('select filename, length(contents) from test.rawfiles')
            result = cur.fetchall()
        # files = os.listdir(TEST_DATA_DIR)
        self.assertEqual(len(result), len(self.files),
                         "Loading folder failed. Expected {0} loaded, got {1}.\n\nResult:\n{2}".format(len(self.files), len(result), pformat(result)))
        names, lengths = zip(*result)
        self.assertGreater(min(lengths), 0, "Some files got zero bytes uploaded: {0}".format(lengths))
