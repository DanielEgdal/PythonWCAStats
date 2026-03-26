import duckdb

class duckdb_reader:
    def __init__(self,tables):
        self.conn = duckdb.connect()
        self.register_views(tables)

    def register_views(self, tables):
        for table in tables:
            try:
                self.conn.sql(f"""
                        CREATE OR REPLACE VIEW {table} AS
                        SELECT 
                            * 
                        FROM
                            read_csv('WCA_export/WCA_export_{table}.tsv',
                                delim = '\t',
                                header = true,
                                quote='',
                                nullstr=['null'],
                                types={{event_id: 'varchar'}}
                            )
                    """)
            except duckdb.duckdb.BinderException:
                self.conn.sql(f"""
                        CREATE OR REPLACE VIEW {table} AS
                        SELECT 
                            * 
                        FROM
                            read_csv('WCA_export/WCA_export_{table}.tsv',
                                delim = '\t',
                                header = true,
                                quote='',
                                nullstr=['null']
                            )
                    """)
            
    def do_query(self,query):
        return self.conn.sql(query)