using SimplePortableDatabase.Storage;
using System.Data;

namespace SimplePortableDatabase
{
    public class Database : IDatabase
    {
        private const string DATA_FILE_FORMAT = "{0}.db";

        public string DataDirectory { get; private set; }
        public string TablesDirectory { get; private set; }
        public string BlobsDirectory { get; private set; }
        public string BackupsDirectory { get; private set; }
        public char Separator { get; private set; }
        public Diagnostics Diagnostics { get; private set; }

        private Dictionary<string, DataTableProperties> dataTablePropertiesDictionary;

        private readonly IObjectListStorage objectListStorage;
        private readonly IDataTableStorage dataTableStorage;
        private readonly IBlobStorage blobStorage;
        private readonly IBackupStorage backupStorage;

        public Database(IObjectListStorage objectListStorage,
            IDataTableStorage dataTableStorage,
            IBlobStorage blobStorage,
            IBackupStorage backupStorage)
        {
            this.objectListStorage = objectListStorage;
            this.dataTableStorage = dataTableStorage;
            this.blobStorage = blobStorage;
            this.backupStorage = backupStorage;
        }

        public void Initialize(string dataDirectory, char separator)
        {
            DataDirectory = dataDirectory;
            TablesDirectory = GetTablesDirectory(dataDirectory);
            BlobsDirectory = GetBlobsDirectory(dataDirectory);
            BackupsDirectory = GetBackupsDirectory(dataDirectory);
            Separator = separator;
            dataTablePropertiesDictionary = new Dictionary<string, DataTableProperties>();
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
            string dataFilePath = ResolveTableFilePath(DataDirectory, tableName);
            Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(tableName);
            dataTableStorage.Initialize(properties, Separator);
            return dataTableStorage.ReadDataTable(dataFilePath, tableName, Diagnostics);
        }

        private DataTableProperties GetDataTableProperties(string tableName)
        {
            return dataTablePropertiesDictionary.ContainsKey(tableName) ?
                dataTablePropertiesDictionary[tableName] : null;
        }

        public List<T> ReadObjectList<T>(string tableName, Func<string[], T> mapObjectFromCsvFields)
        {
            string dataFilePath = ResolveTableFilePath(DataDirectory, tableName);
            Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(tableName);
            objectListStorage.Initialize(properties, Separator);
            return objectListStorage.ReadObjectList(dataFilePath, mapObjectFromCsvFields, Diagnostics);
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

            string dataFilePath = ResolveTableFilePath(DataDirectory, dataTable.TableName);
            Diagnostics = new Diagnostics { LastWriteFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(dataTable.TableName);
            dataTableStorage.Initialize(properties, Separator);
            dataTableStorage.WriteDataTable(dataFilePath, dataTable, Diagnostics);
        }

        public void WriteObjectList<T>(List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            string dataFilePath = ResolveTableFilePath(DataDirectory, tableName);
            Diagnostics = new Diagnostics { LastWriteFilePath = dataFilePath };
            DataTableProperties properties = GetDataTableProperties(tableName);
            objectListStorage.Initialize(properties, Separator);
            objectListStorage.WriteObjectList(dataFilePath, list, tableName, mapCsvFieldIndexToCsvField, Diagnostics);
        }

        public object ReadBlob(string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(DataDirectory, blobName);
            Diagnostics = new Diagnostics { LastReadFilePath = blobFilePath };
            return blobStorage.ReadFromBinaryFile(blobFilePath);
        }

        public void WriteBlob(object blob, string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(DataDirectory, blobName);
            Diagnostics = new Diagnostics { LastWriteFilePath = blobFilePath, LastWriteFileRaw = blob };
            blobStorage.WriteToBinaryFile(blob, blobFilePath);
        }

        public bool WriteBackup(DateTime backupDate)
        {
            bool written = false;
            string backupFilePath = ResolveBackupFilePath(DataDirectory, backupDate);
            
            if (!File.Exists(backupFilePath))
            {
                Diagnostics = new Diagnostics { LastWriteFilePath = backupFilePath };
                backupStorage.WriteToZipFile(DataDirectory, backupFilePath);
                written = true;
            }

            return written;
        }

        public void InitializeDirectory(string dataDirectory)
        {
            Directory.CreateDirectory(dataDirectory);
            Directory.CreateDirectory(TablesDirectory);
            Directory.CreateDirectory(BlobsDirectory);
            // TODO: THE BACKUP DIRECTORY SHOULD BE INDEPENDENTLY CONFIGURED FROM THE DATA DIRECTORY, IN THE APPSETTINGS.JSON FILE.
            Directory.CreateDirectory(BackupsDirectory);
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
            string[] files = backupStorage.GetBackupFiles(BackupsDirectory);
            files = files.OrderBy(f => f).ToArray();
            List<string> deletedBackupFilePaths = new();

            for (int i = 0; i < files.Length - backupsToKeep; i++)
            {
                backupStorage.DeleteBackupFile(files[i]);
                deletedBackupFilePaths.Add(files[i]);
            }

            Diagnostics = new Diagnostics { LastDeletedBackupFilePaths = deletedBackupFilePaths.ToArray() };
        }
    }
}
