import logging

import pymongo

from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


class MongoDBProcessor(Processor):
    def __init__(self, connection_string):
        self.config = {
            'connection_string': connection_string,
        }

    def get_connection(self):
        try:
            client = pymongo.MongoClient(self.config['connection_string'])
            return client
        except Exception as e:
            logger.error(f"Exception occurred while creating mongodb connection with error: {e}")
            raise e

    def test_connection(self):
        try:
            client = self.get_connection()
            try:
                client.server_info()
                return True
            except Exception as e:
                raise Exception("MongoDB Connection Error:: Failed to fetch result from mongodb")
        except Exception as e:
            logger.error(f"Exception occurred while testing mongodb connection with error: {e}")
            raise e

    # TODO (Dipesh): Add support for fetching databases, tables and table assets
    # def fetch_databases(self):
    #     try:
    #         db_databases = []
    #         client = self.get_connection()
    #         databases = client.query('SHOW DATABASES')
    #         for database in databases.result_set:
    #             if database[0] not in ['system', 'INFORMATION_SCHEMA', 'information_schema']:
    #                 db_databases.append(database[0])
    #         client.close()
    #         return db_databases
    #     except Exception as e:
    #         logger.error(f"Exception occurred while fetching clickhouse databases with error: {e}")
    #         raise e

    # def fetch_tables(self, databases: []):
    #     try:
    #         database_tables = {}
    #         client = self.get_connection()
    #         for database in databases:
    #             tables = client.query(f'SHOW TABLES FROM {database}')
    #             db_tables = []
    #             for table in tables.result_set:
    #                 db_tables.append(table[0])
    #             database_tables[database] = db_tables
    #         client.close()
    #         return database_tables
    #     except Exception as e:
    #         logger.error(f"Exception occurred while fetching clickhouse tables with error: {e}")
    #         raise e

    # def fetch_table_details(self, database_table_details):
    #     try:
    #         database_table_metadata = {}
    #         client = self.get_connection()
    #         for database, tables in database_table_details.items():
    #             database_tables = []
    #             for table in tables:
    #                 table_details = {}
    #                 details = client.query(f'DESCRIBE TABLE {database}.{table}')
    #                 columns = details.column_names
    #                 db_columns = []
    #                 for row in details.result_set:
    #                     column_details = {}
    #                     for i, column in enumerate(columns):
    #                         column_details[column] = row[i]
    #                     db_columns.append(column_details)
    #                 table_details[table] = db_columns
    #                 database_tables.append(table_details)

    #             database_table_metadata_dict = {}
    #             for db_tables in database_tables:
    #                 for table, columns in db_tables.items():
    #                     database_table_metadata_dict[table] = columns
    #             database_table_metadata[database] = database_table_metadata_dict
    #         client.close()
    #         return database_table_metadata
    #     except Exception as e:
    #         logger.error(f"Exception occurred while fetching clickhouse table details with error: {e}")
    #         raise e

    def get_query_result(self, database, collection, filters, projection, order_by_field, limit, timeout=120):
        try:
            client = self.get_connection()
            db = client.get_database(database)
            collec = db.get_collection(collection)

            if order_by_field:
                result = collec.find(filters, projection).sort(order_by_field).limit(limit).max_time_ms(timeout * 1000)
            else:
                result = collec.find(filters, projection).limit(limit).max_time_ms(timeout * 1000)
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching mongodb query result with error: {e}")
            raise e

    def get_query_result_count(self, database, collection, filters, limit, timeout=120):
        try:
            client = self.get_connection()
            db = client.get_database(database)
            collec = db.get_collection(collection)
            result = collec.count_documents(filters)
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching mongodb query result with error: {e}")
            raise e
