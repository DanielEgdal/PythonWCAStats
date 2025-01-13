import duckdb

class duckdb_reader:
    def __init__(self,tables):
        self.conn = duckdb.connect()
        self.register_views(tables)

    def register_views(self, tables):
        for table in tables:
            self.conn.sql(f"""
                    CREATE OR REPLACE VIEW {table} AS
                    SELECT 
                        * 
                    FROM
                        read_csv('tables/{table}.tsv',
                            delim = '\t',
                            header = true,
                            quote='',
                            nullstr=['null']
                        )
                """)
            
    def do_query(self,query):
        return self.conn.sql(query)