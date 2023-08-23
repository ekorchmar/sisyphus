import pathlib
import re
import argparse
import logging
import pandas as pd
import sqlalchemy as sa

def _obtain_logger(default_logging_level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("OMOP Upload")
    logger.setLevel(default_logging_level)
    logger.info("Initializing Sisyphus. Developed by Eduard Korchmar for the benefit of entire OHDSI community.")
    return logger

def _obtain_parser()) -> argparse.ArgumentParser:
    user_args_parser = argparse.ArgumentParser(description="Upload OMOP Athena Downloads to an SQL database")

    sql_conn_args = user_args_parser.add_argument_group("SQL Connection Arguments")
    sql_conn_args.add_argument("--sql-dialect", "-d", type=str, default="postgresql", help="SQLAlchemy dialect to use")
    sql_conn_args.add_argument("--sql-host", "-h", type=str, default="127.0.0.1", help="SQL host to connect to")
    sql_conn_args.add_argument("--sql-port", "-p", type=int, default=5432)
    sql_conn_args.add_argument("--sql-user", "-U", type=str, default="postgres")
    sql_conn_args.add_argument("--sql-password", "-W", type=str, default="")
    sql_conn_args.add_argument("--sql-database", "-D", type=str, default="postgres")
    sql_conn_args.add_argument("--schema", "-s", type=str, default="public", help="Schema to upload data to")

    execution_args = user_args_parser.add_argument_group("Execution Arguments")
    execution_args.add_argument("--data-dir", "-d", type=str, help="Directory to read data from")
    execution_args.add_argument("--chunk-size", "-c", type=int, default=100000, help="Number of rows to upload at a time")
    execution_args.add_argument("--ignore-constraints", "-i", action="store_true", default=False, 
                                help="Naively ignore all present constraints. "
                                "If unset, will attempt to drop and recreate after the insert instead")
    execution_args.add_argument("--tables", "-t", type=str, nargs="+", default=[], help="File names to upload. If empty, "
                                "all files in the directory will be processed")
    execution_args.add_argument("--regex-suffix", "-r", type=str, default=r"\.csv", help="Regex to extract table names "
                                "from file names through removal")

    technical_args = user_args_parser.add_argument_group("Technical Arguments")
    technical_args.add_argument("--log-level", "-l", type=str, default="INFO", help="Logging level to use")
    technical_args.add_argument("--dry-run", "-n", action="store_true", default=False, help="Don't actually upload data")
    return user_args_parser
    
def _main() -> None:
    logger = _obtain_logger()
    parser = _obtain_parser()
    
    logger.debug("Parsing user arguments")
    user_args = parser.parse_args()
    
    # Process user arguments to obtain file list, SQL Alchemy engine, etc.
    _process_user_args(user_args)
    
def _process_user_args(user_args: argparse.Namespace) -> argparse.Namespace, sa.Engine:    
    
    # Obtain logging level
    _logger.setLevel(_user_args.log_level.upper())

    # Obtain data directory
    _data_dir = pathlib.Path(_user_args.data_dir)

    # Obtain file names
    if not _user_args.tables:
        _logger.debug("No tables specified (-t), obtaining all files in the directory")
        _user_args.tables = [f.name for f in _data_dir.iterdir() if f.is_file()]

    _file_list = []
    _logger.info("Will process the following files: %s", ", ".join(_user_args.tables))