using System.Runtime.Serialization.Formatters.Binary;

namespace SimplePortableDatabase.Storage
{
    public class BlobStorage : IBlobStorage
    {
        public object ReadFromBinaryFile(string binaryFilePath)
        {
            object result = null;

            if (File.Exists(binaryFilePath))
            {
                using (FileStream fileStream = new(binaryFilePath, FileMode.Open))
                {
                    BinaryFormatter binaryFormatter = new();
                    result = binaryFormatter.Deserialize(fileStream);
                }
            }

            return result;
        }

        public void WriteToBinaryFile(object anObject, string binaryFilePath)
        {
            using (FileStream fileStream = new(binaryFilePath, FileMode.Create))
            {
                BinaryFormatter binaryFormatter = new();
                binaryFormatter.Serialize(fileStream, anObject);
            }
        }
    }
}
