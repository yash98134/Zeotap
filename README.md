# ClickHouse Data Transfer Tool

A web application for bidirectional data transfer between ClickHouse databases and flat files.

## Features

- **Bidirectional Data Flow**:
  - ClickHouse → Flat File (CSV, JSON, TSV)
  - Flat File → ClickHouse

- **ClickHouse Integration**:
  - Connect to ClickHouse using host, port, database, user credentials
  - JWT token authentication support
  - Secure connection support (HTTPS/SSL)

- **Flat File Support**:
  - CSV, TSV, and other delimited text files
  - Custom delimiter configuration
  - Schema auto-detection

- **Advanced Data Handling**:
  - Schema discovery and auto-mapping
  - Column selection
  - Data type inference
  - Table creation
  - Custom column mapping

- **User-Friendly Interface**:
  - Simple and intuitive UI
  - Real-time connection testing
  - Data previews
  - Progress reporting
  - Comprehensive error handling

## Setup Instructions

### Prerequisites

- Python 3.8 or higher
- Node.js 14 or higher
- npm or yarn
- Access to a ClickHouse database

### Backend Setup

1. Clone the repository and navigate to the backend directory:

    ```bash
    git clone https://github.com/yash98134/Zeotap.git
    cd Zeotap
    ```

2. Create and activate a virtual environment:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. Install the required dependencies:

    ```bash
    pip install fastapi uvicorn clickhouse-driver pandas python-multipart
    ```

4. Start the backend server:

    ```bash
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
    ```

### Frontend Setup

1. Navigate to the frontend directory:

    ```bash
    cd ../Frontend
    ```

2. Install the required dependencies:

    ```bash
    npm install
    # or
    yarn install
    ```

3. Start the development server:

    ```bash
    npm run dev
    # or
    yarn start
    ```

    The application will be available at http://localhost:3000 by default.

## Usage

### ClickHouse to Flat File

1. Select the "ClickHouse → Flat File" mode
2. Enter your ClickHouse connection details and test the connection
3. Select the database and table from which you want to export data
4. Select the columns you want to include in the export
5. Choose the export format (CSV, JSON, TSV)
6. Optionally specify a custom filename
7. Click "Export Data" to download the file

### Flat File to ClickHouse

1. Select the "Flat File → ClickHouse" mode
2. Enter your ClickHouse connection details and test the connection
3. Upload a flat file and select the appropriate delimiter
4. Review the automatically detected schema
5. Enter the target table name
6. Configure column mapping if needed
7. Choose whether to create the table if it doesn't exist
8. Click "Import Data" to start the import process

## Environment Variables

The backend supports the following environment variables:

- `CLICKHOUSE_HOST`: Default ClickHouse host (default: "localhost")
- `CLICKHOUSE_PORT`: Default ClickHouse port (default: 9000)
- `CLICKHOUSE_USER`: Default ClickHouse user (default: "default")
- `CLICKHOUSE_PASSWORD`: Default ClickHouse password (default: "")
- `CLICKHOUSE_DATABASE`: Default ClickHouse database (default: "default")
- `CORS_ORIGINS`: Allowed CORS origins, comma-separated (default: "*")

## Security Considerations

- The application by default uses HTTP. For production use, configure HTTPS.
- JWT tokens and passwords are transmitted over the network. Use secure connections in production.
- The application does not implement user authentication. Add an authentication layer for production.

## Error Handling

The application provides detailed error messages for:
- Connection failures
- Authentication issues
- Schema detection problems
- Import/export failures
- File format issues

## License

This project is licensed under the MIT License - see the LICENSE file for details.
