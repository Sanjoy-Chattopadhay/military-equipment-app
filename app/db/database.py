"""Optimized database connection and session management"""
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from contextlib import contextmanager
from app.core.config import settings
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.connection_params = {
            "dbname": settings.DB_NAME,
            "user": settings.DB_USER,
            "password": settings.DB_PASSWORD,
            "host": settings.DB_HOST,
            "port": settings.DB_PORT,
            "sslmode": settings.DB_SSL_MODE
        }

        # Create connection pool for better performance
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                **self.connection_params
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            self.connection_pool = None

    @contextmanager
    def get_connection(self):
        """Context manager for database connections using connection pool"""
        conn = None
        try:
            if self.connection_pool:
                conn = self.connection_pool.getconn()
            else:
                conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise e
        finally:
            if conn:
                if self.connection_pool:
                    self.connection_pool.putconn(conn)
                else:
                    conn.close()

    def execute_query(self, query: str, params=None) -> pd.DataFrame:
        """Execute a query and return DataFrame with optimizations"""
        try:
            with self.get_connection() as conn:
                # Use pandas read_sql with optimizations
                return pd.read_sql(
                    query,
                    conn,
                    params=params,
                    parse_dates=True,  # Auto-parse date columns
                    dtype_backend='numpy_nullable'  # Use nullable dtypes
                )
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise e

db = Database()
