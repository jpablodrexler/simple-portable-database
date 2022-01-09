namespace SimplePortableDatabase.Storage
{
    public interface IObjectListStorage
    {
        void Initialize(DataTableProperties properties, char separator);
        List<T> ReadObjectList<T>(string dataFilePath, Func<string[], T> mapObjectFromCsvFields, Diagnostics diagnostics);
        void WriteObjectList<T>(string dataFilePath, List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField, Diagnostics diagnostics);
    }
}
