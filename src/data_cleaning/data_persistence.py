import logging
import aiomysql

"""
Handle all the operations related to storage and retrieval of cleaned data.
"""
class DataPersistence:
    def __init__(self, pool):
        # self.db_config = db_config
        self.pool = pool

    # async def create_pool(self):
    #     self.pool = await aiomysql.create_pool(**self.db_config)

    async def persist_data_in_mysql(self, cleaner, data):
        """
        Insert or update the cleaned data in MySQL database.
        :param cleaner: The cleaner instance that knows how to get the correct SQL query.
        :param data: The data to be persisted.
        """
        # insert_query, insert_data, update_query, html_field, primary_key_idx = cleaner.get_sql_query(data)
        query_data = cleaner.get_sql_query(data)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    if len(query_data) == 5:
                        # If there's an additional update query and HTML index
                        insert_query, data_to_insert, update_query, html_field, primary_key_idx = query_data
                        await cursor.execute(insert_query, data_to_insert)
                        await cursor.execute(update_query, (html_field, data_to_insert[primary_key_idx]))
                    elif len(query_data) == 2:
                        # If there's only insert data
                        insert_query, data_to_insert = query_data
                        await cursor.execute(insert_query, data_to_insert)
                    else:
                        # Handle unexpected query_data length
                        logging.error(f"Unexpected SQL query data structure: {query_data}")
                        await conn.rollback()
                        return False
                except aiomysql.MySQLError as e:
                    logging.error(f"An error occurred: {e}")
                    await conn.rollback()
                    return False
                else:
                    await conn.commit()
        return True

    async def close_pool(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
