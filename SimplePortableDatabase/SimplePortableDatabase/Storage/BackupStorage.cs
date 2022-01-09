using System.IO.Compression;

namespace SimplePortableDatabase.Storage
{
    internal class BackupStorage
    {
        internal void WriteToZipFile(string dataDirectory, string backupFilePath)
        {
            ZipFile.CreateFromDirectory(dataDirectory, backupFilePath);
        }
    }
}
