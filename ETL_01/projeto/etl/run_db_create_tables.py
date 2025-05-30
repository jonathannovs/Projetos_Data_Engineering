import psycopg2

class RunToDB:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def run_sql_file(self, filepath):
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()

            with open(filepath, 'r') as f:
                sql = f.read()

            cursor.execute(sql)
            conn.commit()

        except Exception as e:
            print(f"Erro ao executar SQL: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
