using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SimplePortableDatabase
{
    internal class ObjectListStorage : BaseStorage
    {
        internal List<T> GetObjectListFromCsv<T>(string csv, DataTableProperties properties, char separator, Func<string[], T> mapObjectFromCsvFields)
        {
            List<T> list = new List<T>();
            bool hasRecord;

            using (StringReader reader = new StringReader(csv))
            {
                string line = reader.ReadLine();

                if (properties != null)
                {
                    string[] headers = GetValuesFromCsvLine(line, properties, separator);

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = GetValuesFromCsvLine(line, properties, separator);
                            list.Add(mapObjectFromCsvFields(fields));
                        }
                    }
                    while (hasRecord);
                }
                else
                {
                    string[] headers = line.Split(separator);

                    do
                    {
                        line = reader.ReadLine();
                        hasRecord = !string.IsNullOrEmpty(line);

                        if (hasRecord)
                        {
                            string[] fields = line.Split(separator);
                            list.Add(mapObjectFromCsvFields(fields));
                        }
                    }
                    while (hasRecord);
                }
            }

            return list;
        }

        internal string GetCsvFromObjectList<T>(List<T> list, string tableName, DataTableProperties properties, char separator, Func<T, int, object> mapCsvFieldIndexToCsvField)
        {
            StringBuilder builder = new StringBuilder();
            
            if (properties == null)
                throw new Exception($"Properties must be defined for the columns in the table {tableName}.");

            for (int i = 0; i < properties.ColumnProperties.Length; i++)
            {
                if (EscapeText(properties, properties.ColumnProperties[i].ColumnName))
                {
                    builder.Append(QUOTE);
                    builder.Append(properties.ColumnProperties[i].ColumnName);
                    builder.Append(QUOTE);
                }
                else
                {
                    builder.Append(properties.ColumnProperties[i].ColumnName);
                }

                if (i < properties.ColumnProperties.Length - 1)
                    builder.Append(separator);
            }

            builder.Append(Environment.NewLine);

            for (int i = 0; i < list.Count; i++)
            {
                T row = list[i];

                for (int j = 0; j < properties.ColumnProperties.Length; j++)
                {
                    if (EscapeText(properties, properties.ColumnProperties[j].ColumnName))
                    {
                        builder.Append(QUOTE);
                        builder.Append(mapCsvFieldIndexToCsvField(row, j));
                        builder.Append(QUOTE);
                    }
                    else
                    {
                        builder.Append(mapCsvFieldIndexToCsvField(row, j));
                    }

                    if (j < properties.ColumnProperties.Length - 1)
                        builder.Append(separator);
                }

                builder.Append(Environment.NewLine);
            }

            return builder.ToString();
        }
    }
}
