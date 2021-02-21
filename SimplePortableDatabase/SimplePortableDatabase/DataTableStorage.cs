﻿using System;
using System.Data;
using System.IO;
using System.Text;

namespace SimplePortableDatabase
{
    internal class DataTableStorage : BaseStorage
    {
        internal DataTableStorage(DataTableProperties properties, char separator) : base(properties, separator)
        {

        }

        internal DataTable GetDataTableFromCsv(string csv, string tableName)
        {
            DataTable table = new DataTable(tableName);
            bool hasRecord;

            using (StringReader reader = new StringReader(csv))
            {
                string line = reader.ReadLine();

                if (this.Properties != null)
                {
                    string[] headers = GetValuesFromCsvLine(line);

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
                            string[] fields = GetValuesFromCsvLine(line);
                            table.Rows.Add(fields);
                        }
                    }
                    while (hasRecord);
                }
                else
                {
                    string[] headers = line.Split(this.Separator);

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
                            string[] fields = line.Split(this.Separator);
                            table.Rows.Add(fields);
                        }
                    }
                    while (hasRecord);
                }

                table.AcceptChanges();
            }

            return table;
        }

        internal string GetCsvFromDataTable(DataTable table)
        {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < table.Columns.Count; i++)
            {
                if (EscapeText(table.Columns[i].ColumnName))
                {
                    builder.Append(QUOTE);
                    builder.Append(table.Columns[i].ColumnName);
                    builder.Append(QUOTE);
                }
                else
                {
                    builder.Append(table.Columns[i].ColumnName);
                }

                if (i < table.Columns.Count - 1)
                    builder.Append(this.Separator);
            }

            builder.Append(Environment.NewLine);

            for (int i = 0; i < table.Rows.Count; i++)
            {
                DataRow row = table.Rows[i];

                for (int j = 0; j < table.Columns.Count; j++)
                {
                    if (EscapeText(table.Columns[j].ColumnName))
                    {
                        builder.Append(QUOTE);
                        builder.Append(row[j]);
                        builder.Append(QUOTE);
                    }
                    else
                    {
                        builder.Append(row[j]);
                    }

                    if (j < table.Columns.Count - 1)
                        builder.Append(this.Separator);
                }

                builder.Append(Environment.NewLine);
            }

            return builder.ToString();
        }
    }
}