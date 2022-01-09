using System.Data;

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
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(tableName);
            return new ObjectListStorage(properties, this.Separator).ReadObjectList(dataFilePath, mapObjectFromCsvFields, this.Diagnostics);
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

            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics = new Diagnostics { LastWriteFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(tableName);
            new ObjectListStorage(properties, this.Separator).WriteObjectList(dataFilePath, list, tableName, mapCsvFieldIndexToCsvField, this.Diagnostics);
        }

        public object ReadBlob(string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(this.DataDirectory, blobName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = blobFilePath };
            return new BlobStorage().ReadFromBinaryFile(blobFilePath);
        }

        public void WriteBlob(object blob, string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(this.DataDirectory, blobName);
            this.Diagnostics = new Diagnostics { LastWriteFilePath = blobFilePath, LastWriteFileRaw = blob };
            new BlobStorage().WriteToBinaryFile(blob, blobFilePath);
        }

        public void WriteBackup(DateTime backupDate)
        {
            // TODO: ADD RETENTION POLICY TO KEEP THE LAST 2 OR 3 BACKUPS (BASED ON DATABASE METADATA).
            string backupFilePath = ResolveBackupFilePath(this.DataDirectory, backupDate);
            
            if (!File.Exists(backupFilePath))
            {
                this.Diagnostics = new Diagnostics { LastWriteFilePath = backupFilePath };
                new BackupStorage().WriteToZipFile(this.DataDirectory, backupFilePath);
            }
        }

        public void InitializeDirectory(string dataDirectory)
        {
            Directory.CreateDirectory(dataDirectory);
            Directory.CreateDirectory(GetTablesDirectory(dataDirectory));
            Directory.CreateDirectory(GetBlobsDirectory(dataDirectory));
            // TODO: THE BACKUP DIRECTORY SHOULD BE INDEPENDENTLY CONFIGURED FROM THE DATA DIRECTORY, IN THE APPSETTINGS.JSON FILE.
            Directory.CreateDirectory(GetBackupsDirectory(dataDirectory));
        }

        public string GetTablesDirectory(string dataDirectory)
        {
            return Path.Combine(dataDirectory, "Tables");
        }

        public string GetBlobsDirectory(string dataDirectory)
        {
            return Path.Combine(dataDirectory, "Blobs");
        }

        public string GetBackupsDirectory(string dataDirectory)
        {
            return dataDirectory + "_Backups";
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

        public string ResolveBackupFilePath(string dataDirectory, DateTime backupDate)
        {
            dataDirectory = !string.IsNullOrEmpty(dataDirectory) ? dataDirectory : string.Empty;
            string fileName = backupDate.ToString("yyyyMMdd") + ".zip";
            return Path.Combine(GetBackupsDirectory(dataDirectory), fileName);
        }

        public void DeleteOldBackups(int backupsToKeep)
        {
            throw new NotImplementedException();
        }
    }
}
