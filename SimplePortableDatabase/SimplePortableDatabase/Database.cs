using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace SimplePortableDatabase
{
    public class Database : IDatabase
    {
        private const string DATA_FILE_FORMAT = "{0}.db";

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
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(tableName);
            return new DataTableStorage(properties, this.Separator).ReadDataTable(dataFilePath, tableName, this.Diagnostics);
        }

        private DataTableProperties GetDataTableProperties(string tableName)
        {
            return this.dataTablePropertiesDictionary.ContainsKey(tableName) ?
                                this.dataTablePropertiesDictionary[tableName] : null;
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
                DataTableProperties properties = GetDataTableProperties(tableName);
                list = new ObjectListStorage(properties, this.Separator).GetObjectListFromCsv(csv, mapObjectFromCsvFields);
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

            string dataFilePath = ResolveTableFilePath(this.DataDirectory, dataTable.TableName);
            this.Diagnostics = new Diagnostics { LastWriteFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(dataTable.TableName);
            new DataTableStorage(properties, this.Separator).WriteDataTable(dataFilePath, dataTable, this.Diagnostics);
        }

        public void WriteObjectList<T>(List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            DataTableProperties properties = GetDataTableProperties(tableName);
            string csv = new ObjectListStorage(properties, this.Separator).GetCsvFromObjectList(list, tableName, mapCsvFieldIndexToCsvField);
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
