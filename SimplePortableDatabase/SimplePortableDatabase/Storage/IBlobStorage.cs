namespace SimplePortableDatabase.Storage
{
    public interface IBlobStorage
    {
        object ReadFromBinaryFile(string binaryFilePath);
        void WriteToBinaryFile(object anObject, string binaryFilePath);
    }
}
