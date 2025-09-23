import pandas as pd


# Add the DuckDB to BigQuery mapper class
class DuckDBToBigQueryMapper:
    """
    Maps DuckDB DataFrame data types to BigQuery external table schema format.
    """

    def __init__(self):
        # Mapping from DuckDB/pandas dtypes to BigQuery types
        self.type_mapping = {
            # Integer types
            "int8": "INTEGER",
            "int16": "INTEGER",
            "int32": "INTEGER",
            "int64": "INTEGER",
            "Int8": "INTEGER",
            "Int16": "INTEGER",
            "Int32": "INTEGER",
            "Int64": "INTEGER",
            # Float types
            "float32": "FLOAT",
            "float64": "FLOAT",
            "Float32": "FLOAT",
            "Float64": "FLOAT",
            # String types
            "object": "STRING",
            "string": "STRING",
            "category": "STRING",
            # Boolean types
            "bool": "BOOLEAN",
            "boolean": "BOOLEAN",
            # Date/time types
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[ns, UTC]": "TIMESTAMP",
            "datetime64": "TIMESTAMP",
            "date": "DATE",
            "time": "TIME",
            "timedelta64[ns]": "TIME",
            # DuckDB specific types
            "VARCHAR": "STRING",
            "TEXT": "STRING",
            "BIGINT": "INTEGER",
            "INTEGER": "INTEGER",
            "SMALLINT": "INTEGER",
            "TINYINT": "INTEGER",
            "DOUBLE": "FLOAT",
            "REAL": "FLOAT",
            "DECIMAL": "NUMERIC",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIME": "TIME",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMPTZ": "TIMESTAMP",
            "UUID": "STRING",
            "BLOB": "BYTES",
            "JSON": "JSON",
        }

    def get_bigquery_type(self, dtype: str) -> str:
        """Convert a DuckDB/pandas dtype to BigQuery type."""
        dtype_str = str(dtype).upper()

        # Handle complex types
        if "DECIMAL" in dtype_str or "NUMERIC" in dtype_str:
            return "NUMERIC"
        elif "TIMESTAMP" in dtype_str:
            return "TIMESTAMP"
        elif "DATETIME" in dtype_str:
            return "TIMESTAMP"
        elif "DATE" in dtype_str:
            return "DATE"
        elif "TIME" in dtype_str:
            return "TIME"
        elif "VARCHAR" in dtype_str or "TEXT" in dtype_str:
            return "STRING"
        elif "INT" in dtype_str or "BIGINT" in dtype_str:
            return "INTEGER"
        elif "FLOAT" in dtype_str or "DOUBLE" in dtype_str or "REAL" in dtype_str:
            return "FLOAT"
        elif "BOOL" in dtype_str:
            return "BOOLEAN"

        return self.type_mapping.get(str(dtype), "STRING")

    def duckdb_describe_to_bq_schema(
        self, describe_df: pd.DataFrame, mode: str = "NULLABLE"
    ) -> list:
        """Convert DuckDB DESCRIBE result to BigQuery schema format."""
        schema = []

        for _, row in describe_df.iterrows():
            field = {
                "name": row["column_name"],
                "type": self.get_bigquery_type(row["column_type"]),
                "mode": mode,
            }
            schema.append(field)

        return schema

    def generate_external_table_ddl(
        self,
        schema: list,
        table_name: str,
        source_uris: list,
        format_type: str = "PARQUET",
        skip_leading_rows: int = 0,
    ) -> str:
        """Generate BigQuery CREATE EXTERNAL TABLE DDL statement."""
        # Build column definitions
        columns = []
        for field in schema:
            column_def = f"  {field['name']} {field['type']}"
            if field["mode"] == "REQUIRED":
                column_def += " NOT NULL"
            columns.append(column_def)

        columns_sql = ",\n".join(columns)

        # Build source URIs
        uris = [f"'{uri}'" for uri in source_uris]
        uris_sql = ",\n    ".join(uris)

        # Build DDL
        ddl = f"""CREATE OR REPLACE EXTERNAL TABLE `{table_name}` (
                    {columns_sql}
                    )
                    OPTIONS (
                      format = '{format_type}',
                      uris = [
                        {uris_sql}
      ]"""

        if format_type.upper() == "CSV" and skip_leading_rows > 0:
            ddl += f",\n  skip_leading_rows = {skip_leading_rows}"

        ddl += "\n);"

        return ddl
