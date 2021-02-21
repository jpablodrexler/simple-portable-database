using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace SimplePortableDatabase
{
    public class Database : IDatabase
    {
        private const string DATA_FILE_FORMAT = "{0}.db";
        private const string QUOTE = "\"";

        public string DataDirectory { get; private set; }
        public char Separator { get; private set; }
        public Diagnostics Diagnostics { get; private set; }

        private Dictionary<string, DataTableProperties> dataTablePropertiesDictionary;

        public void Initialize(string dataDirectory, char separator)
        {
            this.DataDirectory = dataDirectory;
            this.Separator = separator;
            this.dataTablePropertiesDictionary = new Dictionary<string, DataTableProperties>();
            InitializeDirectory(dataDirectory);
        }

        public void SetDataTableProperties(DataTableProperties dataTableProperties)
        {
            if (dataTableProperties == null)
                throw new ArgumentNullException(nameof(dataTableProperties));

            if (dataTableProperties.ColumnProperties == null || dataTableProperties.ColumnProperties.Length == 0)
                throw new ArgumentException("Column properties must not be empty.");

            if (dataTableProperties.ColumnProperties.Count(c => string.IsNullOrWhiteSpace(c.ColumnName)) > 0)
                throw new ArgumentException("All column properties should have a ColumName", nameof(ColumnProperties.ColumnName));

            var group = dataTableProperties.ColumnProperties.GroupBy(c => c.ColumnName).Where(g => g.Count() > 1).FirstOrDefault();
            
            if (group != null)
                throw new ArgumentException("Duplicated column properties.", group.Key);

            dataTablePropertiesDictionary[dataTableProperties.TableName] = dataTableProperties;
        }

        public DataTable ReadDataTable(string tableName)
        {
            DataTable dataTable = null;
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };
            
            if (File.Exists(dataFilePath))
            {
                string csv = File.ReadAllText(dataFilePath);
                this.Diagnostics.LastReadFileRaw = csv;
                dataTable = GetDataTableFromCsv(csv, tableName);
            }

            return dataTable;
        }

        public List<T> ReadObjectList<T>(string tableName, Func<string[], T> mapObjectFromCsvFields)
        {
            List<T> list = new List<T>();
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };

            if (File.Exists(dataFilePath))
            {
                string csv = File.ReadAllText(dataFilePath);
                this.Diagnostics.LastReadFileRaw = csv;
                list = GetObjectListFromCsv(csv, tableName, mapObjectFromCsvFields);
            }
            
            return list;
        }

        public void WriteDataTable(DataTable dataTable)
        {
            if (dataTable == null)
                throw new ArgumentNullException(nameof(dataTable));

            if (dataTable.Columns.Count == 0)
                throw new ArgumentException("DataTable should have at least one column.", nameof(dataTable));

            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                if (string.IsNullOrWhiteSpace(dataTable.Columns[i].ColumnName))
                    throw new ArgumentException("All columns should have a name.", nameof(dataTable));
            }

            string csv = GetCsvFromDataTable(dataTable);
            this.Diagnostics = new Diagnostics { LastWriteFileRaw = csv };
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, dataTable.TableName);
            this.Diagnostics.LastWriteFilePath = dataFilePath;
            File.WriteAllText(dataFilePath, csv);
        }

        public void WriteObjectList<T>(List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            string csv = GetCsvFromObjectList(list, tableName, mapCsvFieldIndexToCsvField);
            this.Diagnostics = new Diagnostics { LastWriteFileRaw = csv };
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics.LastWriteFilePath = dataFilePath;
            File.WriteAllText(dataFilePath, csv);
        }

        public object ReadBlob(string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(this.DataDirectory, blobName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = blobFilePath };
            return ReadFromBinaryFile(blobFilePath);
        }

        public void WriteBlob(object blob, string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(this.DataDirectory, blobName);
            this.Diagnostics = new Diagnostics { LastWriteFilePath = blobFilePath, LastWriteFileRaw = blob };
            WriteToBinaryFile(blob, blobFilePath);
        }

        private string GetCsvFromDataTable(DataTable table)
        {
            StringBuilder builder = new StringBuilder();
            DataTableProperties properties = null;

            if (this.dataTablePropertiesDictionary.ContainsKey(table.TableName))
                properties = this.dataTablePropertiesDictionary[table.TableName];

            for (int i = 0; i < table.Columns.Count; i++)
            {
                if (EscapeText(properties, table.Columns[i].ColumnName))
                {
                    builder.Append(QUOTE);
                    builder.Append(table.Columns[i].ColumnName);
                    builder.Append(QUOTE);
                }
                else
                {
                    builder.Append(table.Columns[i].ColumnName);
                }

                if (i < table.Columns.Count - 1)
                    builder.Append(this.Separator);
            }

            builder.Append(Environment.NewLine);

            for (int i = 0; i < table.Rows.Count; i++)
            {
                DataRow row = table.Rows[i];

                for (int j = 0; j < table.Columns.Count; j++)
                {
                    if (EscapeText(properties, table.Columns[j].ColumnName))
                    {
                        builder.Append(QUOTE);
                        builder.Append(row[j]);
                        builder.Append(QUOTE);
                    }
                    else
                    {
                        builder.Append(row[j]);
                    }

                    if (j < table.Columns.Count - 1)
                        builder.Append(this.Separator);
                }

                builder.Append(Environment.NewLine);
            }

            return builder.ToString();
        }

        private string GetCsvFromObjectList<T>(List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField)
        {
            StringBuilder builder = new StringBuilder();
            DataTableProperties properties = null;

            if (this.dataTablePropertiesDictionary.ContainsKey(tableName))
                properties = this.dataTablePropertiesDictionary[tableName];
            else
                throw new Exception($"Properties must be defined for the columns in the table {tableName}.");

            for (int i = 0; i < properties.ColumnProperties.Length; i++)
            {
                if (EscapeText(properties, properties.ColumnProperties[i].ColumnName))
                {
                    builder.Append(QUOTE);
                    builder.Append(properties.ColumnProperties[i].ColumnName);
                    builder.Append(QUOTE);
                }
                else
                {
                    builder.Append(properties.ColumnProperties[i].ColumnName);
                }

                if (i < properties.ColumnProperties.Length - 1)
                    builder.Append(this.Separator);
            }

            builder.Append(Environment.NewLine);

            for (int i = 0; i < list.Count; i++)
            {
                T row = list[i];

                for (int j = 0; j < properties.ColumnProperties.Length; j++)
                {
                    if (EscapeText(properties, properties.ColumnProperties[j].ColumnName))
                    {
                        builder.Append(QUOTE);
                        builder.Append(mapCsvFieldIndexToCsvField(row, j));
                        builder.Append(QUOTE);
                    }
                    else
                    {
                        builder.Append(mapCsvFieldIndexToCsvField(row, j));
                    }

                    if (j < properties.ColumnProperties.Length - 1)
                        builder.Append(this.Separator);
                }

                builder.Append(Environment.NewLine);
            }

            return builder.ToString();
        }

        private bool EscapeText(DataTableProperties properties, string columnName)
        {
            bool? result = properties?.ColumnProperties.Any(c => string.Compare(c.ColumnName, columnName, StringComparison.OrdinalIgnoreCase) == 0 && c.EscapeText);

            return result.HasValue && result.Value;
        }

        private DataTable GetDataTableFromCsv(string csv, string tableName)
        {
            DataTable table = new DataTable(tableName);
            DataTableProperties properties = null;
            bool hasRecord;

            if (this.dataTablePropertiesDictionary.ContainsKey(table.TableName))
                properties = this.dataTablePropertiesDictionary[table.TableName];

            using (StringReader reader = new StringReader(csv))
            {
                string line = reader.ReadLine();

                if (properties != null)
                {
                    string[] headers = GetValuesFromCsvLine(line, properties);

                    foreach (string header in headers)
                    {
                        table.Columns.Add(header);
                    }

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = GetValuesFromCsvLine(line, properties);
                            table.Rows.Add(fields);
                        }
                    }
                    while (hasRecord);
                }
                else
                {
                    string[] headers = line.Split(this.Separator);

                    foreach (string header in headers)
                    {
                        table.Columns.Add(header);
                    }

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = line.Split(this.Separator);
                            table.Rows.Add(fields);
                        }
                    }
                    while (hasRecord);
                }

                table.AcceptChanges();
            }

            return table;
        }

        private List<T> GetObjectListFromCsv<T>(string csv, string tableName, Func<string[], T> mapObjectFromCsvFields)
        {
            List<T> list = new List<T>();
            DataTableProperties properties = null;
            bool hasRecord;

            if (this.dataTablePropertiesDictionary.ContainsKey(tableName))
                properties = this.dataTablePropertiesDictionary[tableName];

            using (StringReader reader = new StringReader(csv))
            {
                string line = reader.ReadLine();

                if (properties != null)
                {
                    string[] headers = GetValuesFromCsvLine(line, properties);

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = GetValuesFromCsvLine(line, properties);
                            list.Add(mapObjectFromCsvFields(fields));
                        }
                    }
                    while (hasRecord);
                }
                else
                {
                    string[] headers = line.Split(this.Separator);

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = line.Split(this.Separator);
                            list.Add(mapObjectFromCsvFields(fields));
                        }
                    }
                    while (hasRecord);
                }
            }

            return list;
        }

        private string[] GetValuesFromCsvLine(string line, DataTableProperties properties)
        {
            string[] fields = new string[properties.ColumnProperties.Length];
            int startIndex = 0;
            int endIndex;
            
            for (int i = 0; i < properties.ColumnProperties.Length; i++)
            {
                bool escapeText = EscapeText(properties, properties.ColumnProperties[i].ColumnName);

                if (escapeText)
                {
                    endIndex = line.IndexOf(QUOTE + this.Separator, startIndex);
                    startIndex++;
                }
                else
                {
                    endIndex = line.IndexOf(this.Separator, startIndex);
                }

                if (endIndex >= 0 && (endIndex < (line.Length - 1)))
                {
                    string field = escapeText ? line.Substring(startIndex, endIndex - startIndex) : line.Substring(startIndex, endIndex - startIndex);
                    fields[i] = field;
                    startIndex = endIndex + (escapeText ? 2 : 1);
                }
                else if (endIndex == -1)
                {
                    string field = escapeText ? line.Substring(startIndex, line.Length - startIndex - 1) : line.Substring(startIndex);
                    fields[i] = field;
                }
            }

            return fields;
        }

        private object ReadFromBinaryFile(string binaryFilePath)
        {
            object result = null;

            if (File.Exists(binaryFilePath))
            {
                using (FileStream fileStream = new FileStream(binaryFilePath, FileMode.Open))
                {
                    BinaryFormatter binaryFormatter = new BinaryFormatter();
                    result = binaryFormatter.Deserialize(fileStream);
                }
            }

            return result;
        }

        private void WriteToBinaryFile(object anObject, string binaryFilePath)
        {
            using (FileStream fileStream = new FileStream(binaryFilePath, FileMode.Create))
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                binaryFormatter.Serialize(fileStream, anObject);
            }
        }

        public void InitializeDirectory(string dataDirectory)
        {
            Directory.CreateDirectory(dataDirectory);
            Directory.CreateDirectory(GetTablesDirectory(dataDirectory));
            Directory.CreateDirectory(GetBlobsDirectory(dataDirectory));
        }

        public string GetTablesDirectory(string dataDirectory)
        {
            return Path.Combine(dataDirectory, "Tables");
        }

        public string GetBlobsDirectory(string dataDirectory)
        {
            return Path.Combine(dataDirectory, "Blobs");
        }

        public string ResolveTableFilePath(string dataDirectory, string entityName)
        {
            dataDirectory = !string.IsNullOrEmpty(dataDirectory) ? dataDirectory : string.Empty;
            string fileName = string.Format(DATA_FILE_FORMAT, entityName).ToLower();
            return Path.Combine(GetTablesDirectory(dataDirectory), fileName);
        }

        public string ResolveBlobFilePath(string dataDirectory, string blobName)
        {
            return Path.Combine(GetBlobsDirectory(dataDirectory), blobName);
        }
    }
}
