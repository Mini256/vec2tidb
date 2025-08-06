"""Batch update functionality for TiDB tables."""

import logging
import time
from typing import Dict, List, Optional, Tuple, TypedDict
from sqlalchemy import text, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import click
from tqdm import tqdm

from vec2tidb.tidb import create_tidb_engine


logger = logging.getLogger(__name__)

class Page(TypedDict):
    start_key: str
    end_key: str
    page_num: int
    page_size: int


def get_table_pagination(
    engine: Engine, 
    table_name: str, 
    id_column: str, 
    batch_size: int = 5000
) -> List[Page]:
    """
    Get pagination ranges for a table to process in batches.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the source table
        id_column: Name of the ID column
        batch_size: Number of records per batch
        
    Returns:
        List of tuples containing (start_key, end_key) for each batch
    """
    preparer = engine.dialect.identifier_preparer
    table_name = preparer.quote_identifier(table_name)
    id_column = preparer.quote_identifier(id_column)
    pagination_sql = text(f"""
        SELECT
            FLOOR((t1.row_num - 1) / :batch_size) + 1 AS page_num,
            MIN({id_column}) AS start_key,
            MAX({id_column}) AS end_key,
            COUNT(*) AS page_size
        FROM (
            SELECT
                {id_column},
                ROW_NUMBER() OVER (ORDER BY {id_column}) AS row_num
            FROM {table_name}
        ) t1
        GROUP BY page_num
        ORDER BY page_num
    """)

    with Session(engine) as session:
        result = session.execute(pagination_sql, {"batch_size": batch_size})
        
        ranges = []
        for row in result:
            ranges.append(Page(
                start_key=str(row.start_key),
                end_key=str(row.end_key),
                page_num=row.page_num,
                page_size=row.page_size
            ))
        return ranges



def batch_update_within_range(
    engine: Engine,
    source_table: str,
    source_id_column: str,
    target_table: str,
    target_id_column: str,
    column_mapping: Dict[str, str],
    start_key: str,
    end_key: str
) -> int:
    preparer = engine.dialect.identifier_preparer
    source_table = preparer.quote_identifier(source_table)
    target_table = preparer.quote_identifier(target_table)
    source_id_column = preparer.quote_identifier(source_id_column)
    target_id_column = preparer.quote_identifier(target_id_column)

    # Build SET clause
    updated_columns = [
        (preparer.quote_identifier(source_col), preparer.quote_identifier(target_col))
        for source_col, target_col in column_mapping.items()
    ]
    set_clause = ", ".join(
        f"t.{target_col} = s.{source_col}" for source_col, target_col in updated_columns
    )

    # Build SELECT clause for CTE
    source_columns = [source_id_column] + [source_col for source_col, _ in updated_columns]
    select_clause = ", ".join(f"s.{source_column}" for source_column in source_columns)

    # Build BATCH UPDATE SQL
    update_sql = text(f"""
        WITH update_source AS (
            SELECT {select_clause}
            FROM {source_table} s
            WHERE s.{source_id_column} >= :start_key AND s.{source_id_column} <= :end_key
        )
        UPDATE {target_table} AS t
        JOIN update_source s ON s.{source_id_column} = t.{target_id_column}
        SET {set_clause}
    """)

    try:
        with Session(engine) as session:
            result = session.execute(
                update_sql,
                {"start_key": start_key, "end_key": end_key}
            )
            session.commit()
            return result.rowcount
    except SQLAlchemyError as e:
        raise click.ClickException(f"Failed to update batch: {e}")


def batch_update_table(
    engine: Engine,
    source_table: str,
    source_id_column: str,
    target_table: str,
    target_id_column: str,
    column_mapping: Dict[str, str],
    batch_size: int = 5000
) -> int:
    """
    Batch update target table with data from source table.
    
    Args:
        engine: SQLAlchemy engine
        source_table: Name of the source table
        source_id_column: ID column name in source table
        target_table: Name of the target table
        target_id_column: ID column name in target table
        column_mapping: Dictionary mapping source columns to target columns
        batch_size: Number of records per batch
        
    Returns:
        Number of records updated
    """
    pages = get_table_pagination(
        engine, source_table, source_id_column, batch_size
    )

    if not pages:
        click.echo(f"No data found in source table {source_table}")
        return 0

    total_records = sum(page['page_size'] for page in pages)
    total_updated = 0
    
    with tqdm(total=total_records, desc="⏳ Performing batch update", unit=" records") as pbar:
        for page in pages:
            try:
                batch_count = batch_update_within_range(
                    engine=engine,
                    source_table=source_table,
                    source_id_column=source_id_column,
                    target_table=target_table,
                    target_id_column=target_id_column,
                    column_mapping=column_mapping,
                    start_key=page['start_key'],
                    end_key=page['end_key']
                )
                total_updated += batch_count
                pbar.update(batch_count)
            except SQLAlchemyError as e:
                click.echo(f"Error updating batch {page['page_num']}: {e}", err=True)
                raise e

    return total_updated


def validate_table_exists(engine: Engine, table_name: str) -> bool:
    """Validate that a table exists in the database."""
    try:
        with Session(engine) as session:
            # Use parameterized query with proper quoting
            preparer = engine.dialect.identifier_preparer
            quoted_table_name = preparer.quote_identifier(table_name)
            result = session.execute(text(f"SELECT 1 FROM {quoted_table_name} LIMIT 1"))
            return result.fetchone() is not None
    except SQLAlchemyError as e:
        logger.error(f"Error checking if table {table_name} exists: {e}")
        return False


def validate_column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    """Validate that a column exists in the specified table."""
    try:
        with Session(engine) as session:
            preparer = engine.dialect.identifier_preparer
            quoted_table_name = preparer.quote_identifier(table_name)
            result = session.execute(text(f"SHOW COLUMNS FROM {quoted_table_name}"))
            columns = [row[0] for row in result]
            return column_name in columns
    except SQLAlchemyError as e:
        logger.error(f"Error checking if column {column_name} exists in {table_name}: {e}")
        return False


def compact_tiflash_replica(engine: Engine, table_name: str) -> bool:
    try:
        with Session(engine) as session:
            preparer = engine.dialect.identifier_preparer
            quoted_table_name = preparer.quote_identifier(table_name)
            compact_sql = text(f"ALTER TABLE {quoted_table_name} COMPACT;")
            session.execute(compact_sql)
            session.commit()
    except SQLAlchemyError as e:
        raise click.ClickException(f"Failed to compact table {table_name}: {e}")


def batch_update_impl(
    tidb_database_url: str,
    source_table: str,
    source_id_column: str,
    target_table: str,
    target_id_column: str,
    column_mapping: Dict[str, str],
    batch_size: int = 5000,
    compact: bool = False
):
    """
    Implementation of batch update functionality.
    
    Args:
        tidb_database_url: TiDB database connection URL
        source_table: Name of the source table
        source_id_column: ID column name in source table
        target_table: Name of the target table
        target_id_column: ID column name in target table
        column_mapping: Dictionary mapping source columns to target columns
        batch_size: Number of records per batch
        compact: If True, execute ALTER TABLE COMPACT before updating
    """
    # Create engine
    engine = create_tidb_engine(tidb_database_url)
    
    try:
        click.echo(f"🔍 Validating tables and columns")

        # Validate tables exist
        if not validate_table_exists(engine, source_table):
            raise click.BadParameter(f"Source table '{source_table}' does not exist")
        
        if not validate_table_exists(engine, target_table):
            raise click.UsageError(f"Target table '{target_table}' does not exist")
        
        # Validate ID columns exist
        if not validate_column_exists(engine, source_table, source_id_column):
            raise click.BadParameter(f"Source ID column '{source_id_column}' does not exist in table '{source_table}'")
        
        if not validate_column_exists(engine, target_table, target_id_column):
            raise click.BadParameter(f"Target ID column '{target_id_column}' does not exist in table '{target_table}'")
        
        # Validate column mapping
        for source_col, target_col in column_mapping.items():
            if not validate_column_exists(engine, source_table, source_col):
                raise click.BadParameter(f"Source column '{source_col}' does not exist in table '{source_table}'")

            if not validate_column_exists(engine, target_table, target_col):
                raise click.BadParameter(f"Target column '{target_col}' does not exist in table '{target_table}'")

        if compact:
            click.echo(f"📦 Compacting tiflash replica for table {target_table}")
            start_time = time.time()
            compact_tiflash_replica(engine, target_table)
            elapsed_time = time.time() - start_time
            click.echo(f"✅ Compacted tiflash replica of {target_table} in {elapsed_time:.2f} seconds")
        
        start_time = time.time()
        total_updated = batch_update_table(
            engine=engine,
            source_table=source_table,
            source_id_column=source_id_column,
            target_table=target_table,
            target_id_column=target_id_column,
            column_mapping=column_mapping,
            batch_size=batch_size
        )
        elapsed_time = time.time() - start_time
        click.echo(f"🎊 Successfully updated {total_updated} records in {elapsed_time:.2f} seconds")
            
    finally:
        # Ensure engine is properly disposed
        engine.dispose() 