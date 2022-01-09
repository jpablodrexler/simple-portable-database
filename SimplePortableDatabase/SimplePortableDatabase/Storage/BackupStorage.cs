using System.IO.Compression;

namespace SimplePortableDatabase.Storage
{
    public class BackupStorage : IBackupStorage
    {
        public string[] GetBackupFiles(string backupDirectory)
        {
            return Directory.GetFiles(backupDirectory);
        }

        public void WriteToZipFile(string dataDirectory, string backupFilePath)
        {
            ZipFile.CreateFromDirectory(dataDirectory, backupFilePath);
        }

        public void DeleteBackupFile(string backupFilePath)
        {
            File.Delete(backupFilePath);
        }
    }
}
