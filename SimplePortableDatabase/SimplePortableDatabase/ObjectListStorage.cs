using System.Text;

namespace SimplePortableDatabase
{
    internal class ObjectListStorage : BaseCsvStorage
    {
        internal ObjectListStorage(DataTableProperties properties, char separator) : base(properties, separator)
        {

        }

        internal List<T> ReadObjectList<T>(string dataFilePath, Func<string[], T> mapObjectFromCsvFields, Diagnostics diagnostics)
        {
            List<T> list = new();
            
            if (File.Exists(dataFilePath))
            {
                string csv = File.ReadAllText(dataFilePath);
                diagnostics.LastReadFileRaw = csv;
                list = this.GetObjectListFromCsv(csv, mapObjectFromCsvFields);
            }

            return list;
        }

        internal void WriteObjectList<T>(string dataFilePath, List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField, Diagnostics diagnostics)
        {
            string csv = this.GetCsvFromObjectList(list, tableName, mapCsvFieldIndexToCsvField);
            diagnostics.LastWriteFileRaw = csv;
            File.WriteAllText(dataFilePath, csv);
        }

        private List<T> GetObjectListFromCsv<T>(string csv, Func<string[], T> mapObjectFromCsvFields)
        {
            List<T> list = new();
            bool hasRecord;

            using (StringReader reader = new(csv))
            {
                string line = reader.ReadLine();

                if (this.Properties != null)
                {
                    string[] headers = GetValuesFromCsvLine(line);

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = GetValuesFromCsvLine(line);
                            list.Add(mapObjectFromCsvFields(fields));
                        }
                    }
                    while (hasRecord);
                }
                else
                {
                    string[] headers = line.Split(this.Separator);

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = line.Split(this.Separator);
                            list.Add(mapObjectFromCsvFields(fields));
                        }
                    }
                    while (hasRecord);
                }
            }

            return list;
        }

        private string GetCsvFromObjectList<T>(List<T> list, string tableName, Func<T, int, object> mapCsvFieldIndexToCsvField)
        {
            StringBuilder builder = new();
            
            if (this.Properties == null)
                throw new Exception($"Properties must be defined for the columns in the table {tableName}.");

            for (int i = 0; i < this.Properties.ColumnProperties.Length; i++)
            {
                if (EscapeText(this.Properties.ColumnProperties[i].ColumnName))
                {
                    builder.Append(QUOTE);
                    builder.Append(this.Properties.ColumnProperties[i].ColumnName);
                    builder.Append(QUOTE);
                }
                else
                {
                    builder.Append(this.Properties.ColumnProperties[i].ColumnName);
                }

                if (i < this.Properties.ColumnProperties.Length - 1)
                    builder.Append(this.Separator);
            }

            builder.Append(Environment.NewLine);

            for (int i = 0; i < list.Count; i++)
            {
                T row = list[i];

                for (int j = 0; j < this.Properties.ColumnProperties.Length; j++)
                {
                    if (EscapeText(this.Properties.ColumnProperties[j].ColumnName))
                    {
                        builder.Append(QUOTE);
                        builder.Append(mapCsvFieldIndexToCsvField(row, j));
                        builder.Append(QUOTE);
                    }
                    else
                    {
                        builder.Append(mapCsvFieldIndexToCsvField(row, j));
                    }

                    if (j < this.Properties.ColumnProperties.Length - 1)
                        builder.Append(this.Separator);
                }

                builder.Append(Environment.NewLine);
            }

            return builder.ToString();
        }
    }
}
