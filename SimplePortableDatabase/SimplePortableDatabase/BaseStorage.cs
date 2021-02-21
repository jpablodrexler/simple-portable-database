using System;
using System.Linq;

namespace SimplePortableDatabase
{
    internal abstract class BaseStorage
    {
        protected const string QUOTE = "\"";

        protected string[] GetValuesFromCsvLine(string line, DataTableProperties properties, char separator)
        {
            string[] fields = new string[properties.ColumnProperties.Length];
            int startIndex = 0;
            int endIndex;

            for (int i = 0; i < properties.ColumnProperties.Length; i++)
            {
                bool escapeText = EscapeText(properties, properties.ColumnProperties[i].ColumnName);

                if (escapeText)
                {
                    endIndex = line.IndexOf(QUOTE + separator, startIndex);
                    startIndex++;
                }
                else
                {
                    endIndex = line.IndexOf(separator, startIndex);
                }

                if (endIndex >= 0 && (endIndex < (line.Length - 1)))
                {
                    string field = escapeText ? line.Substring(startIndex, endIndex - startIndex) : line.Substring(startIndex, endIndex - startIndex);
                    fields[i] = field;
                    startIndex = endIndex + (escapeText ? 2 : 1);
                }
                else if (endIndex == -1)
                {
                    string field = escapeText ? line.Substring(startIndex, line.Length - startIndex - 1) : line.Substring(startIndex);
                    fields[i] = field;
                }
            }

            return fields;
        }

        protected bool EscapeText(DataTableProperties properties, string columnName)
        {
            bool? result = properties?.ColumnProperties.Any(c => string.Compare(c.ColumnName, columnName, StringComparison.OrdinalIgnoreCase) == 0 && c.EscapeText);

            return result.HasValue && result.Value;
        }
    }
}
