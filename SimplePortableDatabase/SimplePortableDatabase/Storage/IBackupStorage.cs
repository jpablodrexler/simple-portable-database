namespace SimplePortableDatabase.Storage
{
    public interface IBackupStorage
    {
        string[] GetBackupFiles(string backupDirectory);
        void WriteToZipFile(string dataDirectory, string backupFilePath);
        void DeleteBackupFile(string backupFilePath);
    }
}
