import os
import csv
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from fastapi import FastAPI, HTTPException, Query, Body, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from clickhouse_driver import Client
import uvicorn
import clickhouse_connect
import pandas as pd
import io

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="ClickHouse Data Transfer Tool")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
# class ClickHouseConnection(BaseModel):
#     host: str = "vxpvp25vas.ap-south-1.aws.clickhouse.cloud"
#     port: int = 8443
#     database: str = "default"
#     user: str = "default"
#     jwt_token: str = "_2dXHA.nP3DtP"
#     secure: bool = True
class ClickHouseConnection(BaseModel):
    host: str = "vxpvp25vas.ap-south-1.aws.clickhouse.cloud"
    port: int = 8443
    database: str = "default"
    user: str = "default"
    jwt_token: str = "_2dXHA.nP3DtP"
    secure: bool = True

class ExportRequest(BaseModel):
    connection: ClickHouseConnection
    query: str
    table_name: str
    format: str = "csv"  # csv, json, tsv
    filename: Optional[str] = None
    include_headers: bool = True
    selected_columns: List[str] = []

class ImportRequest(BaseModel):
    connection: ClickHouseConnection
    table_name: str
    create_table: bool = False
    column_mapping: Dict[str, str] = {}  # file_column: clickhouse_column

class FileSchema(BaseModel):
    columns: List[str]
    sample_data: List[List[str]]

# Helper functions
def get_clickhouse_client(connection: ClickHouseConnection):
    # Clean up the host value
    host = connection.host.strip()
    
    # Remove protocol prefix if present
    if "://" in host:
        host = host.split("://", 1)[1]
    
    # Remove port if included in host
    if ":" in host:
        host = host.split(":", 1)[0]
    
    return clickhouse_connect.get_client(
        host=host,
        port=connection.port,
        username=connection.user,
        password=connection.jwt_token,
        database=connection.database,
        secure=connection.secure
    )

def detect_file_schema(file_path, delimiter=',', encoding='utf-8'):
    try:
        # For CSV files
        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, nrows=5)
        columns = df.columns.tolist()
        sample_data = df.values.tolist()
        return FileSchema(columns=columns, sample_data=sample_data)
    except Exception as e:
        logger.error(f"Error detecting schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error detecting file schema: {str(e)}")

def guess_clickhouse_types(df):
    """Guess appropriate ClickHouse column types based on pandas DataFrame"""
    type_mapping = {}
    
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            if pd.api.types.is_integer_dtype(df[column]):
                type_mapping[column] = "Int64"
            else:
                type_mapping[column] = "Float64"
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            type_mapping[column] = "DateTime"
        elif pd.api.types.is_bool_dtype(df[column]):
            type_mapping[column] = "UInt8"  # ClickHouse doesn't have boolean, 0/1 are used
        else:
            type_mapping[column] = "String"
    
    return type_mapping

# API Routes
@app.get("/")
def read_root():
    return {"status": "ok", "service": "ClickHouse Data Transfer Tool"}

@app.post("/test-connection")
def test_connection(connection: ClickHouseConnection):
    try:
        client = get_clickhouse_client(connection)
        result = client.query("SELECT 1")
        rows = query_result.result_rows
        return {"status": "success", "message": "Connection successful"}
    except Exception as e:
        logger.error(f"Connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")

@app.post("/clickhouse/databases")
def list_databases(connection: ClickHouseConnection):
    try:
        client = get_clickhouse_client(connection)
        result = client.query("SHOW DATABASES")
        return {"databases": [row[0] for row in result if row[0] not in ['system', 'information_schema']]}
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/clickhouse/tables")
def list_tables(connection: ClickHouseConnection):
    try:
        client = get_clickhouse_client(connection)
        result = client.query(f"SHOW TABLES FROM {connection.database}")
        return {"tables": [row[0] for row in result]}
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/clickhouse/table-schema")
def get_table_schema(connection: ClickHouseConnection, table_name: str = Form(...)):
    try:
        client = get_clickhouse_client(connection)
        result = client.query(f"DESCRIBE TABLE {connection.database}.{table_name}")
        schema = [{"name": row[0], "type": row[1], "default_type": row[2], "default_expression": row[3]} for row in result]
        
        # Get sample data
        sample_data = client.query(f"SELECT * FROM {connection.database}.{table_name} LIMIT 5", with_column_types=True)
        columns = [col[0] for col in sample_data[1]]
        data = sample_data[0]
        
        return {
            "schema": schema,
            "columns": columns,
            "sample_data": data
        }
    except Exception as e:
        logger.error(f"Schema error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Schema error: {str(e)}")

@app.post("/file/schema")
async def get_file_schema(
    file: UploadFile = File(...),
    delimiter: str = Form(','),
):
    try:
        # Save uploaded file temporarily
        temp_file_path = f"/tmp/{file.filename}"
        with open(temp_file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Detect schema
        schema = detect_file_schema(temp_file_path, delimiter)
        
        # Read a bit more data for type inference
        df = pd.read_csv(temp_file_path, delimiter=delimiter, nrows=100)
        inferred_types = guess_clickhouse_types(df)
        
        # Clean up
        os.remove(temp_file_path)
        
        return {
            "columns": schema.columns,
            "sample_data": schema.sample_data,
            "inferred_types": inferred_types
        }
    except Exception as e:
        logger.error(f"File schema error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File schema error: {str(e)}")

@app.post("/export/clickhouse-to-file")
async def export_clickhouse_to_file(request: ExportRequest):
    try:
        client = get_clickhouse_client(request.connection)
        
        # Build query with selected columns if specified
        query = request.query
        if request.selected_columns and "*" in query:
            # Replace * with specific columns
            select_clause = ", ".join(request.selected_columns)
            query = query.replace("*", select_clause)
        
        # query the query
        result = client.query(query, with_column_types=True)
        
        # Extract data and column names
        data = result[0]
        columns = [col[0] for col in result[1]]
        
        # Generate filename if not provided
        if not request.filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            request.filename = f"export_{request.table_name}_{timestamp}.{request.format}"
        
        # Prepare the file content
        if request.format.lower() == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            if request.include_headers:
                writer.writerow(columns)
            writer.writerows(data)
            file_content = output.getvalue()
            content_type = "text/csv"
            
        elif request.format.lower() == "json":
            if request.include_headers:
                json_data = [dict(zip(columns, row)) for row in data]
            else:
                json_data = [list(row) for row in data]
            file_content = json.dumps(json_data, default=str, indent=2)
            content_type = "application/json"
            
        elif request.format.lower() == "tsv":
            output = io.StringIO()
            writer = csv.writer(output, delimiter='\t')
            if request.include_headers:
                writer.writerow(columns)
            writer.writerows(data)
            file_content = output.getvalue()
            content_type = "text/tab-separated-values"
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {request.format}")
        
        return {
            "success": True,
            "filename": request.filename,
            "format": request.format,
            "rows_exported": len(data),
            "content": file_content,
            "content_type": content_type
        }
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")

@app.post("/import/file-to-clickhouse")
async def import_file_to_clickhouse(
    connection_data: str = Form(...),
    table_name: str = Form(...),
    create_table: bool = Form(False),
    column_mapping: str = Form(...),
    file: UploadFile = File(...),
    delimiter: str = Form(',')
):
    try:
        # Parse connection and mapping data
        connection = ClickHouseConnection.parse_raw(connection_data)
        mapping = json.loads(column_mapping)
        
        # Get ClickHouse client
        client = get_clickhouse_client(connection)
        
        # Save uploaded file temporarily
        temp_file_path = f"/tmp/{file.filename}"
        with open(temp_file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Read the file with pandas
        df = pd.read_csv(temp_file_path, delimiter=delimiter)
        
        # Apply column mapping
        if mapping:
            # Keep only mapped columns and rename them
            df = df[list(mapping.keys())].rename(columns=mapping)
        
        # Create table if requested
        if create_table:
            # Infer types
            type_mapping = guess_clickhouse_types(df)
            
            # Build CREATE TABLE statement
            columns_def = []
            for col_name, col_type in type_mapping.items():
                columns_def.append(f"`{col_name}` {col_type}")
            
            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {connection.database}.{table_name} (
                {', '.join(columns_def)}
            ) ENGINE = MergeTree() ORDER BY tuple()
            """
            
            client.query(create_stmt)
        
        # Convert DataFrame to format suitable for ClickHouse bulk insert
        columns = df.columns.tolist()
        values = df.values.tolist()
        
        # Insert data in batches
        batch_size = 10000
        total_rows = len(values)
        rows_inserted = 0
        
        for i in range(0, total_rows, batch_size):
            batch = values[i:i+batch_size]
            client.query(
                f"INSERT INTO {connection.database}.{table_name} ({', '.join([f'`{col}`' for col in columns])}) VALUES",
                batch
            )
            rows_inserted += len(batch)
        
        # Clean up
        os.remove(temp_file_path)
        
        return {
            "success": True,
            "table_name": table_name,
            "rows_imported": rows_inserted,
            "message": f"Successfully imported {rows_inserted} rows into {table_name}"
        }
        
    except Exception as e:
        logger.error(f"Import error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Import error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)