using System.Data;

namespace SimplePortableDatabase.Storage
{
    public interface IDataTableStorage
    {
        void Initialize(DataTableProperties properties, char separator);
        DataTable ReadDataTable(string dataFilePath, string tableName, Diagnostics diagnostics);
        void WriteDataTable(string dataFilePath, DataTable dataTable, Diagnostics diagnostics);
    }
}
