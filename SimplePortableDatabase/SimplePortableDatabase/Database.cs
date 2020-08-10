using System.Data;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace SimplePortableDatabase
{
    public class Database : IDatabase
    {
        private const string DATA_FILE_FORMAT = "{0}.db";

        public string DataDirectory { get; private set; }
        public char Separator { get; private set; }
        public Diagnostics Diagnostics { get; private set; }

        public void Initialize(string dataDirectory, char separator)
        {
            this.DataDirectory = dataDirectory;
            this.Separator = separator;
            InitializeDirectory(dataDirectory);
        }

        public DataTable ReadDataTable(string tableName)
        {
            DataTable dataTable = null;
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, tableName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = dataFilePath };
            
            if (File.Exists(dataFilePath))
            {
                string csv = File.ReadAllText(dataFilePath);
                this.Diagnostics.LastReadFileRaw = csv;
                dataTable = GetDataTableFromCsv(csv, this.Separator, tableName);
            }

            return dataTable;
        }

        public void WriteDataTable(DataTable dataTable)
        {
            string csv = GetCsvFromDataTable(dataTable, this.Separator);
            this.Diagnostics = new Diagnostics { LastWriteFileRaw = csv };
            string dataFilePath = ResolveTableFilePath(this.DataDirectory, dataTable.TableName);
            this.Diagnostics.LastWriteFilePath = dataFilePath;
            File.WriteAllText(dataFilePath, csv);
        }

        public object ReadBlob(string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(this.DataDirectory, blobName);
            this.Diagnostics = new Diagnostics { LastReadFilePath = blobFilePath };
            return ReadFromBinaryFile(blobFilePath);
        }

        public void WriteBlob(object blob, string blobName)
        {
            string blobFilePath = ResolveBlobFilePath(this.DataDirectory, blobName);
            this.Diagnostics = new Diagnostics { LastWriteFilePath = blobFilePath, LastWriteFileRaw = blob };
            WriteToBinaryFile(blob, blobFilePath);
        }

        private string GetCsvFromDataTable(DataTable table, char separator)
        {
            StringBuilder builder = new StringBuilder();
            string[] headers = new string[table.Columns.Count];

            for (int i = 0; i < table.Columns.Count; i++)
            {
                headers[i] = table.Columns[i].ColumnName;
            }

            builder.AppendLine(string.Join(separator.ToString(), headers));

            for (int i = 0; i < table.Rows.Count; i++)
            {
                DataRow row = table.Rows[i];
                string line = string.Join(separator.ToString(), row.ItemArray);
                builder.AppendLine(line);
            }

            return builder.ToString();
        }

        private DataTable GetDataTableFromCsv(string csv, char separator, string tableName)
        {
            DataTable table = new DataTable(tableName);

            using (StringReader reader = new StringReader(csv))
            {
                string line = reader.ReadLine();
                string[] headers = line.Split(separator);
                bool hasRecord;

                foreach (string header in headers)
                {
                    table.Columns.Add(header);
                }

                do
                {
                    line = reader.ReadLine();
                    hasRecord = !string.IsNullOrEmpty(line);

                    if (hasRecord)
                    {
                        string[] fields = line.Split(separator);
                        table.Rows.Add(fields);
                    }
                }
                while (hasRecord);

                table.AcceptChanges();
            }

            return table;
        }

        private object ReadFromBinaryFile(string binaryFilePath)
        {
            object result = null;

            if (File.Exists(binaryFilePath))
            {
                using (FileStream fileStream = new FileStream(binaryFilePath, FileMode.Open))
                {
                    BinaryFormatter binaryFormatter = new BinaryFormatter();
                    result = binaryFormatter.Deserialize(fileStream);
                }
            }

            return result;
        }

        private void WriteToBinaryFile(object anObject, string binaryFilePath)
        {
            using (FileStream fileStream = new FileStream(binaryFilePath, FileMode.Create))
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                binaryFormatter.Serialize(fileStream, anObject);
            }
        }

        public void InitializeDirectory(string dataDirectory)
        {
            Directory.CreateDirectory(dataDirectory);
            Directory.CreateDirectory(GetTablesDirectory(dataDirectory));
            Directory.CreateDirectory(GetBlobsDirectory(dataDirectory));
        }

        public string GetTablesDirectory(string dataDirectory)
        {
            return Path.Combine(dataDirectory, "Tables");
        }

        public string GetBlobsDirectory(string dataDirectory)
        {
            return Path.Combine(dataDirectory, "Blobs");
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
    }
}
