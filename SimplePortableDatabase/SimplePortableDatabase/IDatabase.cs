using System.Data;

namespace SimplePortableDatabase
{
    public interface IDatabase
    {
        string DataDirectory { get; }
        char Separator { get; }
        Diagnostics Diagnostics { get; }
        void Initialize(string dataDirectory, char separator);
        DataTable ReadDataTable(string tableName);
        void WriteDataTable(DataTable dataTable);
        string GetBlobsDirectory(string dataDirectory);
        string GetCsvFromDataTable(DataTable table, char separator);
        DataTable GetDataTableFromCsv(string csv, char separator, string tableName);
        string GetTablesDirectory(string dataDirectory);
        void InitializeDirectory(string dataDirectory);
        object ReadBlob(string blobName);
        string ResolveBlobFilePath(string dataDirectory, string thumbnailsFileName);
        string ResolveTableFilePath(string dataDirectory, string entityName);
        void WriteBlob(object blob, string blobName);
    }
}
