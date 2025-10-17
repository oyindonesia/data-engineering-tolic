import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
                    ]
        """

        if format_type.upper() == "CSV" and skip_leading_rows > 0:
            ddl += f",\n  skip_leading_rows = {skip_leading_rows}"

        ddl += "\n);"

        return ddl


def compare_bigquery_schemas_dict(
    old_schema: list | dict, new_schema: list | dict
) -> dict:
    """
    Compare two BigQuery schemas (as dict/list objects) and identify changes.
    This version works in-memory without files - perfect for Airflow/GKE.

    Args:
        old_schema: Original schema as list or dict
        new_schema: Updated schema as list or dict

    Returns:
        Dictionary with schema changes categorized
    """
    try:
        # Convert schema arrays to dictionaries for easier comparison
        old_cols = (
            {col["name"]: col for col in old_schema}
            if isinstance(old_schema, list)
            else old_schema
        )
        new_cols = (
            {col["name"]: col for col in new_schema}
            if isinstance(new_schema, list)
            else new_schema
        )

        all_column_names = set(old_cols.keys()) | set(new_cols.keys())

        results = {
            "added_columns": {},  # New columns added
            "removed_columns": {},  # Columns that were removed
            "modified_columns": {},  # Columns with changed properties
            "unchanged_columns": {},  # Columns that remained the same
        }

        for col_name in all_column_names:
            if col_name in old_cols and col_name in new_cols:
                if old_cols[col_name] == new_cols[col_name]:
                    results["unchanged_columns"][col_name] = new_cols[col_name]
                else:
                    results["modified_columns"][col_name] = {
                        "old": old_cols[col_name],
                        "new": new_cols[col_name],
                    }
            elif col_name in new_cols:
                results["added_columns"][col_name] = new_cols[col_name]
            else:
                results["removed_columns"][col_name] = old_cols[col_name]

        return results

    except Exception as e:
        logger.error(f"Error comparing schemas: {e}", exc_info=True)
        raise


def union_bigquery_schemas(old_schema: list, new_schema: list) -> dict:
    """
    Create a union of two schemas (append-only, never delete columns).
    This preserves historical data even when source columns are deleted.

    Args:
        old_schema: Existing schema (what's currently in BigQuery/GCS)
        new_schema: New schema (from current query)

    Returns:
        {
            'union_schema': List of all columns from both schemas,
            'added_columns': Columns only in new_schema (newly added),
            'removed_from_source': Columns only in old_schema (deleted from source),
            'type_conflicts': Columns with different types,
            'unchanged_columns': Columns present in both with same type
        }

    Example:
        old = [{"name": "id", "type": "INTEGER"}, {"name": "email", "type": "STRING"}]
        new = [{"name": "id", "type": "INTEGER"}, {"name": "phone", "type": "STRING"}]

        result = union_bigquery_schemas(old, new)
        # union_schema will have: [id, email, phone]
        # email is kept even though deleted from source!
    """
    try:
        # Convert to dicts for easier lookup
        old_cols = {col["name"]: col for col in old_schema}
        new_cols = {col["name"]: col for col in new_schema}

        all_column_names = set(old_cols.keys()) | set(new_cols.keys())

        union_schema = []
        added_columns = {}
        removed_from_source = {}
        type_conflicts = {}
        unchanged_columns = {}

        # Process all columns in sorted order for consistency
        for col_name in sorted(all_column_names):
            if col_name in new_cols:
                # Column exists in new schema
                col_def = new_cols[col_name].copy()

                if col_name in old_cols:
                    # Column exists in both - check for type changes
                    if old_cols[col_name]["type"] != new_cols[col_name]["type"]:
                        # Type conflict detected!
                        type_conflicts[col_name] = {
                            "old_type": old_cols[col_name]["type"],
                            "new_type": new_cols[col_name]["type"],
                        }
                        # Keep the old type to maintain compatibility with historical data
                        col_def = old_cols[col_name].copy()
                        logger.warning(
                            f"Type conflict for column '{col_name}': "
                            f"{old_cols[col_name]['type']} → {new_cols[col_name]['type']}. "
                            f"Keeping old type for compatibility."
                        )
                    else:
                        # Column unchanged
                        unchanged_columns[col_name] = col_def
                else:
                    # New column added
                    added_columns[col_name] = col_def

                union_schema.append(col_def)

            elif col_name in old_cols:
                # Column only in old schema (removed from source)
                # But we keep it for historical data!
                col_def = old_cols[col_name].copy()
                removed_from_source[col_name] = col_def
                union_schema.append(col_def)

        return {
            "union_schema": union_schema,
            "added_columns": added_columns,
            "removed_from_source": removed_from_source,
            "type_conflicts": type_conflicts,
            "unchanged_columns": unchanged_columns,
        }

    except Exception as e:
        logger.error(f"Error creating union schema: {e}", exc_info=True)
        raise
