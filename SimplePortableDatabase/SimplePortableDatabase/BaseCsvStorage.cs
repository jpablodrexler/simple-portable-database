using System;
using System.Linq;

namespace SimplePortableDatabase
{
    internal abstract class BaseCsvStorage
    {
        protected const string QUOTE = "\"";
        protected DataTableProperties Properties { get; set; }
        protected char Separator { get; set; }

        internal BaseCsvStorage(DataTableProperties properties, char separator)
        {
            this.Properties = properties;
            this.Separator = separator;
        }

        protected string[] GetValuesFromCsvLine(string line)
        {
            string[] fields = new string[this.Properties.ColumnProperties.Length];
            int startIndex = 0;
            int endIndex;

            for (int i = 0; i < this.Properties.ColumnProperties.Length; i++)
            {
                bool escapeText = EscapeText(this.Properties.ColumnProperties[i].ColumnName);

                if (escapeText)
                {
                    endIndex = line.IndexOf(QUOTE + this.Separator, startIndex);
                    startIndex++;
                }
                else
                {
                    endIndex = line.IndexOf(this.Separator, startIndex);
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

        protected bool EscapeText(string columnName)
        {
            bool? result = this.Properties?.ColumnProperties.Any(c => string.Compare(c.ColumnName, columnName, StringComparison.OrdinalIgnoreCase) == 0 && c.EscapeText);

            return result.HasValue && result.Value;
        }
    }
}
