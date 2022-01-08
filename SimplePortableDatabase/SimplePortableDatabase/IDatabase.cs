using System.Data;

namespace SimplePortableDatabase
{
    public interface IDatabase
    {
        string DataDirectory { get; }
        char Separator { get; }
        Diagnostics Diagnostics { get; }
        void Initialize(string dataDirectory, char separator);
        void SetDataTableProperties(DataTableProperties dataTableProperties);
        DataTable ReadDataTable(string tableName);
        void WriteDataTable(DataTable dataTable);
        List<T> ReadObjectList<T>(string tableName, Func<string[], T> mapObjectFromCsvFields);
        void WriteObjectList<T>(List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField);
        string GetBlobsDirectory(string dataDirectory);
        string GetTablesDirectory(string dataDirectory);
        string GetBackupsDirectory(string dataDirectory);
        void InitializeDirectory(string dataDirectory);
        object ReadBlob(string blobName);
        string ResolveBlobFilePath(string dataDirectory, string thumbnailsFileName);
        string ResolveTableFilePath(string dataDirectory, string entityName);
        string ResolveBackupFilePath(string dataDirectory, DateTime backupDate);
        void WriteBlob(object blob, string blobName);
        void WriteBackup(DateTime backupDate);
    }
}
