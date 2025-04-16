import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import './App.css';

// API base URL
const API_BASE_URL = 'http://localhost:8000';

function App() {
  // Mode selection state
  const [mode, setMode] = useState('clickhouse-to-file'); // 'clickhouse-to-file' or 'file-to-clickhouse'

  // ClickHouse connection state
  const [connection, setConnection] = useState({
    host: 'localhost',
    port: 9000,
    database: '',
    user: 'default',
    jwt_token: '',
    secure: false
  });
  
  // Connection status
  const [connectionStatus, setConnectionStatus] = useState(null);
  
  // Database schema state
  const [databases, setDatabases] = useState(["default"]);
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [tableSchema, setTableSchema] = useState({ columns: [], sample_data: [] });
  const [selectedColumns, setSelectedColumns] = useState([]);
  
  // File handling state
  const [file, setFile] = useState(null);
  const [fileDelimiter, setFileDelimiter] = useState(',');
  const [fileSchema, setFileSchema] = useState({ columns: [], sample_data: [], inferred_types: {} });
  
  // Export state
  const [exportFormat, setExportFormat] = useState('csv');
  const [exportFilename, setExportFilename] = useState('');
  const [includeHeaders, setIncludeHeaders] = useState(true);
  
  // Import state
  const [createTable, setCreateTable] = useState(true);
  const [columnMapping, setColumnMapping] = useState({});
  
  // Process status
  const [processStatus, setProcessStatus] = useState(null);
  
  // Download link ref
  const downloadLinkRef = useRef(null);
  
  // Error handling
  const [error, setError] = useState(null);
  
  // Test connection to ClickHouse
  const testConnection = async () => {
    try {
      setConnectionStatus({ status: 'testing', message: 'Testing connection...' });
      
      const response = await axios.post(`${API_BASE_URL}/test-connection`, connection);
      
      setConnectionStatus({ 
        status: 'success', 
        message: 'Connection successful' 
      });
      
      // Fetch databases
      fetchDatabases();
      
    } catch (err) {
      setConnectionStatus({ 
        status: 'error', 
        message: `Connection failed: ${err.response?.data?.detail || err.message}` 
      });
    }
  };
  
  // Fetch databases
  const fetchDatabases = async () => {
    try {
      const response = await axios.post(`${API_BASE_URL}/clickhouse/databases`, connection);
      setDatabases(response.data.databases);
      setError(null);
    } catch (err) {
      setError('Failed to fetch databases: ' + (err.response?.data?.detail || err.message));
    }
  };
  
  // Fetch tables when database is selected
  const fetchTables = async () => {
    try {
      const response = await axios.post(`${API_BASE_URL}/clickhouse/tables`, connection);
      setTables(response.data.tables);
      setError(null);
    } catch (err) {
      setError('Failed to fetch tables: ' + (err.response?.data?.detail || err.message));
    }
  };
  
  // Fetch table schema
  const fetchTableSchema = async () => {
    try {
      const formData = new FormData();
      formData.append('table_name', selectedTable);
      
      // Convert connection object to JSON string
      formData.append('connection', JSON.stringify(connection));
      
      const response = await axios.post(
        `${API_BASE_URL}/clickhouse/table-schema`, 
        formData,
        {
          headers: {
            'Content-Type': 'multipart/form-data'
          }
        }
      );
      
      setTableSchema({
        columns: response.data.columns,
        sample_data: response.data.sample_data,
        schema: response.data.schema
      });
      
      // By default, select all columns
      setSelectedColumns(response.data.columns);
      
      setError(null);
    } catch (err) {
      setError('Failed to fetch table schema: ' + (err.response?.data?.detail || err.message));
    }
  };
  
  // Handle file selection
  const handleFileChange = async (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
      
      if (mode === 'file-to-clickhouse') {
        try {
          const formData = new FormData();
          formData.append('file', selectedFile);
          formData.append('delimiter', fileDelimiter);
          
          const response = await axios.post(
            `${API_BASE_URL}/file/schema`, 
            formData,
            {
              headers: {
                'Content-Type': 'multipart/form-data'
              }
            }
          );
          
          setFileSchema({
            columns: response.data.columns,
            sample_data: response.data.sample_data,
            inferred_types: response.data.inferred_types
          });
          
          // Initialize column mapping (1:1 by default)
          const initialMapping = {};
          response.data.columns.forEach(col => {
            initialMapping[col] = col;
          });
          setColumnMapping(initialMapping);
          
          setError(null);
        } catch (err) {
          setError('Failed to analyze file: ' + (err.response?.data?.detail || err.message));
        }
      }
    }
  };
  
  // Handle checkbox change for column selection
  const handleColumnSelect = (column) => {
    setSelectedColumns(prev => {
      if (prev.includes(column)) {
        return prev.filter(col => col !== column);
      } else {
        return [...prev, column];
      }
    });
  };
  
  // Handle column mapping change
  const handleColumnMappingChange = (fileColumn, clickhouseColumn) => {
    setColumnMapping(prev => ({
      ...prev,
      [fileColumn]: clickhouseColumn
    }));
  };
  
  // Export data from ClickHouse to file
  const handleExport = async () => {
    try {
      setProcessStatus({ status: 'running', message: 'Exporting data...' });
      
      const response = await axios.post(`${API_BASE_URL}/export/clickhouse-to-file`, {
        connection,
        query: `SELECT * FROM ${connection.database}.${selectedTable}`,
        table_name: selectedTable,
        format: exportFormat,
        filename: exportFilename || undefined,
        include_headers: includeHeaders,
        selected_columns: selectedColumns
      });
      
      // Create download link
      const blob = new Blob([response.data.content], { type: response.data.content_type });
      const url = window.URL.createObjectURL(blob);
      
      // Trigger download
      const downloadLink = downloadLinkRef.current;
      downloadLink.href = url;
      downloadLink.download = response.data.filename;
      downloadLink.click();
      
      // Cleanup
      window.URL.revokeObjectURL(url);
      
      setProcessStatus({ 
        status: 'success', 
        message: `Successfully exported ${response.data.rows_exported} rows to ${response.data.filename}` 
      });
      
    } catch (err) {
      setProcessStatus({ 
        status: 'error', 
        message: `Export failed: ${err.response?.data?.detail || err.message}` 
      });
    }
  };
  
  // Import data from file to ClickHouse
  const handleImport = async () => {
    try {
      setProcessStatus({ status: 'running', message: 'Importing data...' });
      
      const formData = new FormData();
      formData.append('file', file);
      formData.append('delimiter', fileDelimiter);
      formData.append('connection_data', JSON.stringify(connection));
      formData.append('table_name', selectedTable);
      formData.append('create_table', createTable);
      formData.append('column_mapping', JSON.stringify(columnMapping));
      
      const response = await axios.post(
        `${API_BASE_URL}/import/file-to-clickhouse`, 
        formData,
        {
          headers: {
            'Content-Type': 'multipart/form-data'
          }
        }
      );
      
      setProcessStatus({ 
        status: 'success', 
        message: response.data.message
      });
      
    } catch (err) {
      setProcessStatus({ 
        status: 'error', 
        message: `Import failed: ${err.response?.data?.detail || err.message}` 
      });
    }
  };
  
  // Effect to fetch tables when database changes
  useEffect(() => {
    if (connection.database) {
      fetchTables();
    }
  }, [connection.database]);
  
  // Effect to fetch table schema when table is selected
  useEffect(() => {
    if (selectedTable) {
      fetchTableSchema();
    }
  }, [selectedTable]);
  
  return (
    <div className="app">
      <header>
        <h1>ClickHouse Data Transfer Tool</h1>
      </header>
      
      {error && (
        <div className="error-message">
          {error}
          <button onClick={() => setError(null)}>Dismiss</button>
        </div>
      )}
      
      <div className="mode-selector">
        <h2>Select Transfer Direction</h2>
        <div className="mode-buttons">
          <button 
            className={mode === 'clickhouse-to-file' ? 'active' : ''}
            onClick={() => setMode('clickhouse-to-file')}
          >
            ClickHouse → Flat File
          </button>
          <button 
            className={mode === 'file-to-clickhouse' ? 'active' : ''}
            onClick={() => setMode('file-to-clickhouse')}
          >
            Flat File → ClickHouse
          </button>
        </div>
      </div>
      
      <div className="container">
        <div className="panel connection-panel">
          <h2>ClickHouse Connection</h2>
          
          <div className="form-group">
            <label>Host:</label>
            <input 
              type="text"
              value={connection.host}
              onChange={(e) => setConnection({...connection, host: e.target.value})}
              placeholder="localhost"
            />
          </div>
          
          <div className="form-group">
            <label>Port:</label>
            <input 
              type="number"
              value={connection.port}
              onChange={(e) => setConnection({...connection, port: parseInt(e.target.value)})}
              placeholder="9000"
            />
          </div>
          
          <div className="form-group">
            <label>User:</label>
            <input 
              type="text"
              value={connection.user}
              onChange={(e) => setConnection({...connection, user: e.target.value})}
              placeholder="default"
            />
          </div>
          
          <div className="form-group">
            <label>JWT Token / Password:</label>
            <input 
              type="password"
              value={connection.jwt_token}
              onChange={(e) => setConnection({...connection, jwt_token: e.target.value})}
              placeholder="Enter JWT token or password"
            />
          </div>
          
          <div className="form-group">
            <label>Database:</label>
            <select 
              value={connection.database}
              onChange={(e) => setConnection({...connection, database: e.target.value})}
            >
              <option value="">Select Database</option>
              <option value="">default</option>
              {databases.map(db => (
                <option key={db} value={db}>{db}</option>
              ))}
            </select>
          </div>
          
          <div className="form-group checkbox">
            <input 
              type="checkbox"
              id="secure-connection"
              checked={connection.secure}
              onChange={(e) => setConnection({...connection, secure: e.target.checked})}
            />
            <label htmlFor="secure-connection">Use secure connection (HTTPS/SSL)</label>
          </div>
          
          <button 
            className="primary-button"
            onClick={testConnection}
          >
            Test Connection
          </button>
          
          {connectionStatus && (
            <div className={`connection-status ${connectionStatus.status}`}>
              {connectionStatus.message}
            </div>
          )}
        </div>
        
        {/* Source/Target Selection Panel */}
        <div className="panel data-panel">
          {mode === 'clickhouse-to-file' ? (
            <>
              <h2>ClickHouse Source</h2>
              
              <div className="form-group">
                <label>Table:</label>
                <select 
                  value={selectedTable}
                  onChange={(e) => setSelectedTable(e.target.value)}
                  disabled={!connection.database || tables.length === 0}
                >
                  <option value="">Select Table</option>
                  {tables.map(table => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
              </div>
              
              {tableSchema.columns.length > 0 && (
                <div className="schema-viewer">
                  <h3>Select Columns</h3>
                  
                  <div className="column-selector">
                    <label>
                      <input 
                        type="checkbox"
                        checked={selectedColumns.length === tableSchema.columns.length}
                        onChange={() => {
                          if (selectedColumns.length === tableSchema.columns.length) {
                            setSelectedColumns([]);
                          } else {
                            setSelectedColumns([...tableSchema.columns]);
                          }
                        }}
                      />
                      Select All
                    </label>
                    
                    {tableSchema.columns.map(column => (
                      <div key={column} className="column-item">
                        <label>
                          <input 
                            type="checkbox"
                            checked={selectedColumns.includes(column)}
                            onChange={() => handleColumnSelect(column)}
                          />
                          {column}
                        </label>
                      </div>
                    ))}
                  </div>
                  
                  <h3>Sample Data</h3>
                  <div className="sample-data">
                    <table>
                      <thead>
                        <tr>
                          {tableSchema.columns.map(column => (
                            <th key={column}>{column}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {tableSchema.sample_data.map((row, rowIndex) => (
                          <tr key={rowIndex}>
                            {row.map((cell, cellIndex) => (
                              <td key={cellIndex}>{String(cell)}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          ) : (
            <>
              <h2>File Source</h2>
              
              <div className="form-group">
                <label>Select File:</label>
                <input 
                  type="file"
                  onChange={handleFileChange}
                  accept=".csv,.tsv,.txt"
                />
              </div>
              
              <div className="form-group">
                <label>File Delimiter:</label>
                <select
                  value={fileDelimiter}
                  onChange={(e) => setFileDelimiter(e.target.value)}
                >
                  <option value=",">Comma (,)</option>
                  <option value="\t">Tab (\t)</option>
                  <option value=";">Semicolon (;)</option>
                  <option value="|">Pipe (|)</option>
                </select>
              </div>
              
              {file && fileSchema.columns.length > 0 && (
                <div className="schema-viewer">
                  <h3>File Schema</h3>
                  
                  <div className="sample-data">
                    <table>
                      <thead>
                        <tr>
                          {fileSchema.columns.map(column => (
                            <th key={column}>{column}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {fileSchema.sample_data.map((row, rowIndex) => (
                          <tr key={rowIndex}>
                            {row.map((cell, cellIndex) => (
                              <td key={cellIndex}>{String(cell)}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  
                  <div className="form-group">
                    <label>Target ClickHouse Table:</label>
                    <input 
                      type="text"
                      value={selectedTable}
                      onChange={(e) => setSelectedTable(e.target.value)}
                      placeholder="Enter table name"
                    />
                  </div>
                  
                  <div className="form-group checkbox">
                    <input 
                      type="checkbox"
                      id="create-table"
                      checked={createTable}
                      onChange={(e) => setCreateTable(e.target.checked)}
                    />
                    <label htmlFor="create-table">Create table if not exists</label>
                  </div>
                  
                  <h3>Column Mapping</h3>
                  <div className="column-mapping">
                    <table>
                      <thead>
                        <tr>
                          <th>File Column</th>
                          <th>ClickHouse Column</th>
                          <th>Inferred Type</th>
                        </tr>
                      </thead>
                      <tbody>
                        {fileSchema.columns.map(column => (
                          <tr key={column}>
                            <td>{column}</td>
                            <td>
                              <input 
                                type="text"
                                value={columnMapping[column] || ''}
                                onChange={(e) => handleColumnMappingChange(column, e.target.value)}
                                placeholder="ClickHouse column name"
                              />
                            </td>
                            <td>{fileSchema.inferred_types[column] || 'String'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
        
        {/* Export/Import Options Panel */}
        <div className="panel options-panel">
          {mode === 'clickhouse-to-file' ? (
            <>
              <h2>Export Options</h2>
              
              <div className="form-group">
                <label>Export Format:</label>
                <select
                  value={exportFormat}
                  onChange={(e) => setExportFormat(e.target.value)}
                >
                  <option value="csv">CSV</option>
                  <option value="json">JSON</option>
                  <option value="tsv">TSV</option>
                </select>
              </div>
              
              <div className="form-group">
                <label>Filename (optional):</label>
                <input 
                  type="text"
                  value={exportFilename}
                  onChange={(e) => setExportFilename(e.target.value)}
                  placeholder={`${selectedTable || 'export'}_${new Date().toISOString().slice(0,10)}.${exportFormat}`}
                />
              </div>
              
              <div className="form-group checkbox">
                <input 
                  type="checkbox"
                  id="include-headers"
                  checked={includeHeaders}
                  onChange={(e) => setIncludeHeaders(e.target.checked)}
                />
                <label htmlFor="include-headers">Include headers</label>
              </div>
              
              <button 
                className="primary-button"
                onClick={handleExport}
                disabled={!selectedTable || selectedColumns.length === 0}
              >
                Export Data
              </button>
              
              {/* Hidden download link */}
              <a 
                ref={downloadLinkRef} 
                style={{ display: 'none' }}
              >
                Download
              </a>
            </>
          ) : (
            <>
              <h2>Import Options</h2>
              
              <button 
                className="primary-button"
                onClick={handleImport}
                disabled={!file || !selectedTable || Object.keys(columnMapping).length === 0}
              >
                Import Data
              </button>
            </>
          )}
          
          {processStatus && (
            <div className={`process-status ${processStatus.status}`}>
              {processStatus.message}
              {processStatus.status !== 'running' && (
                <button onClick={() => setProcessStatus(null)}>Dismiss</button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;