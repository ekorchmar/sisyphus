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
    sql_conn_args.add_argument("--schema", "-s", type=str, default="", help="Schema to upload data to. Defaults to the "
                               "user default schema")

    execution_args = user_args_parser.add_argument_group("Execution Arguments")
    execution_args.add_argument("--data-dir", "-d", type=str, help="Directory to read data from")
    execution_args.add_argument("--chunk-size", "-c", type=int, default=100000, help="Number of rows to upload "
                                "at a time")
    execution_args.add_argument("--ignore-constraints", "-i", action="store_true", default=False, 
                                help="Naively ignore all present constraints. "
                                "If unset, will attempt to drop and recreate after the insert instead")
    execution_args.add_argument("--tables", "-t", type=str, nargs="+", default=[], help="File names to upload. If empty, "
                                "all files in the directory will be processed")
    execution_args.add_argument("--regex-suffix", "-r", type=str, default=r"\.csv", help="Regex to extract table names "
                                "from file names through removal")
    execution_args.add_argument("--no-headers", "-H", action="store_true", default=False, help="Files have no headers")
    execution_args.add_argument("--sep", "-e", type=str, default=",", help="Separator to assume when redaing CSV files")

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
    _process_user_args(logger, user_args)
    
def _process_user_args(logger: logging.Logger, user_args: argparse.Namespace) -> tuple[
        argparse.Namespace,
        sa.Engine,
        sa.Metadata,
        dict[pathlib.Path, str]
    ]:    
    
    # Obtain logging level
    logger.setLevel(user_args.log_level.upper())

    # Obtain data directory
    data_dir = pathlib.Path(user_args.data_dir)

    # Obtain file names
    if not user_args.tables:
        logger.debug("No tables specified (-t), obtaining all files in the directory")
        user_args.tables = [f.name for f in data_dir.iterdir() if f.is_file()]
    else:
        logger.debug("Tables specified (-t), checking if they exist in the directory")
        for table in user_args.tables:
            if not (data_dir / table).is_file():
                raise FileNotFoundError(f"File {table} not found in {data_dir}")
                   
    logger.info(f"Will process the following files: {', '.join(user_args.tables)}")
    
    # Obtain expected table names from file paths
    logger.debug("Obtaining table names from file names")
    regex = re.compile(user_args.regex_suffix)
    table_names = {}
    for file_name in user_args.tables:
        # Test regex match
        match = regex.search(file_name)
        if not match:
            logger.error(f"File name {file_name} does not match the regex {user_args.regex_suffix}")
            logger.debug(f"Regex: {user_args.regex_suffix}")
            raise ValueError(f"File name {file_name} does not match the regex {user_args.regex_suffix}")
        
        # Obtain table name
        table_name = file_name[:match.start()]
        table_names[file_name] = table_name
        
    logger.debug(f"Obtained table names: {table_names.values()}")
    
    # Obtain SQL Alchemy engine
    logger.debug("Obtaining SQL Alchemy engine")
    engine_url = sa.URL.create(
        drivername=user_args.sql_dialect,
        username=user_args.sql_user,
        password=user_args.sql_password or None,
        host=user_args.sql_host,
        port=int(user_args.sql_port),
        database=user_args.sql_database
    )
    logger.debug(f"Engine URL: {engine_url}")
    engine = sa.create_engine(engine_url)
    
    # Test connection
    logger.debug("Testing connection")
    with engine.connect():
        logger.debug("Connection successful")
        
    # Obtain metadata object
    logger.debug("Reflecting metadata")
    metadata = sa.MetaData()
    metadata.reflect(bind=engine, schema=user_args.schema or None)
    
    # Test if all tables exist in the database
    logger.debug("Testing if all tables exist in the database")
    for file_name, table_name in table_names.items():
        if table_name not in metadata.tables:
            logger.error(f"Table {table_name} corresponding to {file_name} is not found in the database")
            raise ValueError(f"Table {table_name} not found in the database")