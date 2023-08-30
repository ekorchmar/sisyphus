import pathlib
import re
import argparse
import logging
import pandas as pd
import sqlalchemy as sa
from concurrent.futures import ThreadPoolExecutor

from typing import Type

_DTYPE_CONVERSION_DICT: dict[Type, Type] = {
    sa.Integer: pd.Int64Dtype,
    sa.Float: pd.Float64Dtype,
    sa.String: pd.StringDtype,
    sa.DateTime: pd.DatetimeTZDtype,
    }

def _main() -> None:
    logger = _obtain_logger()
    parser = _obtain_arg_parser()
    
    logger.debug("Parsing user arguments")
    user_args = parser.parse_args()
    
    # Process user arguments to obtain file list, SQL Alchemy engine, etc.
    user_args, engine, metadata, table_names = _process_user_args(logger, user_args)
    
    if user_args.execute_first:
        _execute_sql(logger, engine, metadata, user_args.execute_first, user_args.dry_run)
    
    logger.info(f"Starting upload with {user_args.threads} threads")
    with ThreadPoolExecutor(max_workers=user_args.threads) as executor:
        for file_name, table_name in table_names.items():
            executor.submit(_process_file, logger, user_args, engine, metadata, file_name, table_name)
            
    if user_args.execute_last:
        _execute_sql(logger, engine, metadata, user_args.execute_last, user_args.dry_run)

def _obtain_logger(default_logging_level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("OMOP Upload")
    logger.setLevel(default_logging_level)
    logger.info("Initializing Sisyphus. Developed by Eduard Korchmar for the benefit of entire OHDSI community.")
    return logger

def _obtain_arg_parser() -> argparse.ArgumentParser:
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
    execution_args.add_argument("--threads", "-T", type=int, default=5, help="Number of threads to use")
    execution_args.add_argument("--tables", "-t", type=str, nargs="+", default=[], help="File names to upload. If empty, "
                                "all files in the directory will be processed")
    execution_args.add_argument("--regex-suffix", "-r", type=str, default=r"\.csv", help="Regex to extract table names "
                                "from file names through removal")
    execution_args.add_argument("--sep", "-e", type=str, default=",", help="Separator to assume when reading CSV files")
    execution_args.add_argument("--execute_first", "-x", type=str, default="", help="Path to SQL script to execute " 
                                "before uploading")
    execution_args.add_argument("--execute_last", "-X", type=str, default="", help="Path to SQL script to execute "
                                "after uploading")


    technical_args = user_args_parser.add_argument_group("Technical Arguments")
    technical_args.add_argument("--log-level", "-l", type=str, default="INFO", help="Logging level to use")
    technical_args.add_argument("--dry-run", "-n", action="store_true", default=False, help="Don't actually upload data")
    return user_args_parser

def _process_user_args(logger: logging.Logger, user_args: argparse.Namespace) -> tuple[
        argparse.Namespace,
        sa.Engine,
        sa.MetaData,
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
            err_string = f"File name {file_name} does not match the regex {user_args.regex_suffix}"
            logger.error(err_string)
            logger.debug(f"Regex: {user_args.regex_suffix}")
            raise ValueError(err_string)
        
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
    logger.info(f"Obtained metadata for schema {metadata.schema} containing {len(metadata.tables)} tables")
    
    # Test if all required tables exist in the database
    logger.debug("Testing if all the tables exist in the database")
    for file_name, table_name in table_names.items():
        if table_name not in metadata.tables:
            err_string = f"Table {table_name} corresponding to {file_name} is not found in the database"
            logger.error(err_string)
            raise ValueError(err_string)

    return user_args, engine, metadata, table_names

def _process_file(
        logger: logging.Logger,
        user_args: argparse.Namespace,
        engine: sa.Engine,
        metadata: sa.MetaData,
        file_name: pathlib.Path,
        table_name: str
    ):
    logger.info(f"Processing file {file_name} / table {table_name}")
    
    # Obtain dtype dict from table metadata
    logger.debug("Obtaining dtype dict from table metadata")
    sql_type_column_dict = {}
    for column in metadata.tables[table_name].columns:
        sql_type_column_dict[column.name] = column.type
        
    # Apply type conversion to the dtype dict
    dtype_column_dict = {col: _convert_dtype(type_) for col, type_ in sql_type_column_dict.items()}
    
    # Create file io reader through pandas
    pd_io = pd.read_csv(
        filepath_or_buffer=file_name,
        sep=user_args.sep or ',',
        low_memory=True,
        dtype=dtype_column_dict,
        chunksize=user_args.chunk_size
    )
    
    for i, chunk in enumerate(pd_io):
        logger.info(f"{table_name}: Uploading chunk {i} of length {len(chunk)}")
        if user_args.dry_run:
            logger.info("Dry run, skipping upload")
        else:
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=metadata.schema or None,
                if_exists="append",
                index=False,
                method="multi"
            )
        

def _execute_sql(
        logger: logging.Logger,
        engine: sa.Engine,
        metadata: sa.MetaData,
        script_path: pathlib.Path | str,
        dry_run: bool = False
    ) -> None:
    logger.info(f"Executing script {script_path}")
    
    if dry_run:
        logger.info("Dry run, skipping execution")
        return
    
    with engine.connect() as conn:
        with pathlib.Path(script_path).open("r") as script_file:
            sql = sa.text(script_file.read())
            conn.execute(sql)

    # Reflect metadata anew to account for changes
    logger.debug("Reflecting metadata")
    metadata.reflect(bind=engine, schema=metadata.schema or None)

def _convert_dtype(type_: Type) -> Type:
    for sql_type, dtype in _DTYPE_CONVERSION_DICT.items():
        if isinstance(type_(), sql_type):
            return dtype

if __name__ == "__main__":
    _main()