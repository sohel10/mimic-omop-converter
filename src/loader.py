import sqlite3
from pathlib import Path


class OMOPLoader:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        print(f"Connected to {db_path}")

    def init_schema(self):
        schema_path = Path(__file__).parent.parent / "config" / "omop_schema.sql"
        with open(schema_path) as f:
            self.conn.executescript(f.read())
        print("Schema initialized")

    def load_table(self, spark_df, table_name, chunk_size=500000):
        """Write Spark DataFrame to SQLite using toLocalIterator."""
        print(f"  Writing {table_name}...")
        columns = spark_df.columns
        total = 0
        batch = []

        # Repartition to keep each partition under maxResultSize
        repartitioned = spark_df.repartition(20)

        for row in repartitioned.toLocalIterator():
            batch.append(tuple(row[c] for c in columns))
            if len(batch) >= chunk_size:
                self._write_batch(batch, table_name, columns)
                total += len(batch)
                print(f"    {total:,} rows written...")
                batch = []

        if batch:
            self._write_batch(batch, table_name, columns)
            total += len(batch)

        print(f"  ✓ {table_name}: {total:,} rows")
        return total

    def _write_batch(self, batch, table_name, columns):
        placeholders = ','.join(['?'] * len(columns))
        col_str = ','.join(columns)
        sql = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"
        self.conn.executemany(sql, batch)
        self.conn.commit()

    def close(self):
        self.conn.close()
