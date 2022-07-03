using Autofac.Extras.Moq;
using FluentAssertions;
using Moq;
using SimplePortableDatabase.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Xunit;

namespace SimplePortableDatabase.Tests
{
    public class DatabaseTest
    {
        private string dataDirectory;
        
        public DatabaseTest()
        {
            dataDirectory = Path.GetDirectoryName(typeof(DatabaseTest).Assembly.Location);
            dataDirectory = Path.Combine(dataDirectory, "TestFiles");
        }

        [Theory]
        [InlineData(@"C:\Data\JPPhotoManager", @"C:\Data\JPPhotoManager\Tables\assets.db")]
        [InlineData("", @"Tables\assets.db")]
        [InlineData(null, @"Tables\assets.db")]
        public void ResolveTableFilePathTest(string directory, string expected)
        {
            IDatabase database = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            string result = database.ResolveTableFilePath(directory, "assets");

            result.Should().Be(expected);
        }

        [Fact]
        public void WriteDataTable_AllColumnsWithUnescapedTextWithoutDataTableProperties()
        {
            string expected = "FolderId;FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;Hash\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            DataTable table = new(tableName);
            table.Columns.Add("FolderId");
            table.Columns.Add("FileName");
            table.Columns.Add("FileSize");
            table.Columns.Add("ImageRotation");
            table.Columns.Add("PixelWidth");
            table.Columns.Add("PixelHeight");
            table.Columns.Add("ThumbnailPixelWidth");
            table.Columns.Add("ThumbnailPixelHeight");
            table.Columns.Add("ThumbnailCreationDateTime");
            table.Columns.Add("Hash");

            DataRow row = table.NewRow();
            row["FolderId"] = "876283c6-780e-4ad5-975c-be63044c087a";
            row["FileName"] = "20200720175810_3.jpg";
            row["FileSize"] = "363888";
            row["ImageRotation"] = "Rotate0";
            row["PixelWidth"] = "1920";
            row["PixelHeight"] = "1080";
            row["ThumbnailPixelWidth"] = "200";
            row["ThumbnailPixelHeight"] = "112";
            row["ThumbnailCreationDateTime"] = "25/07/2020 9:45:47";
            row["Hash"] = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4";
            table.Rows.Add(row);

            row = table.NewRow();
            row["FolderId"] = "876283c6-780e-4ad5-975c-be63044c087a";
            row["FileName"] = "20200720175816_3.jpg";
            row["FileSize"] = "343633";
            row["ImageRotation"] = "Rotate0";
            row["PixelWidth"] = "1920";
            row["PixelHeight"] = "1080";
            row["ThumbnailPixelWidth"] = "200";
            row["ThumbnailPixelHeight"] = "112";
            row["ThumbnailCreationDateTime"] = "25/07/2020 9:45:47";
            row["Hash"] = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124";
            table.Rows.Add(row);

            table.AcceptChanges();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.WriteDataTable(table);

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            string content = File.ReadAllText(filePath);
            content.Should().Be(expected);
        }

        [Fact]
        public void WriteDataTable_AllColumnsWithEscapedText()
        {
            string expected = "\"FolderId\";\"FileName\";\"FileSize\";\"ImageRotation\";\"PixelWidth\";\"PixelHeight\";\"ThumbnailPixelWidth\";\"ThumbnailPixelHeight\";\"ThumbnailCreationDateTime\";\"Description\";\"Hash\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175810_3.jpg\";\"363888\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"First file description\";\"4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175816_3.jpg\";\"343633\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"Second file description; Includes separator character escaped.\";\"0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\"\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            DataTable table = new(tableName);
            table.Columns.Add("FolderId");
            table.Columns.Add("FileName");
            table.Columns.Add("FileSize");
            table.Columns.Add("ImageRotation");
            table.Columns.Add("PixelWidth");
            table.Columns.Add("PixelHeight");
            table.Columns.Add("ThumbnailPixelWidth");
            table.Columns.Add("ThumbnailPixelHeight");
            table.Columns.Add("ThumbnailCreationDateTime");
            table.Columns.Add("Description");
            table.Columns.Add("Hash");

            DataRow row = table.NewRow();
            row["FolderId"] = "876283c6-780e-4ad5-975c-be63044c087a";
            row["FileName"] = "20200720175810_3.jpg";
            row["FileSize"] = "363888";
            row["ImageRotation"] = "Rotate0";
            row["PixelWidth"] = "1920";
            row["PixelHeight"] = "1080";
            row["ThumbnailPixelWidth"] = "200";
            row["ThumbnailPixelHeight"] = "112";
            row["ThumbnailCreationDateTime"] = "25/07/2020 9:45:47";
            row["Description"] = "First file description";
            row["Hash"] = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4";
            table.Rows.Add(row);

            row = table.NewRow();
            row["FolderId"] = "876283c6-780e-4ad5-975c-be63044c087a";
            row["FileName"] = "20200720175816_3.jpg";
            row["FileSize"] = "343633";
            row["ImageRotation"] = "Rotate0";
            row["PixelWidth"] = "1920";
            row["PixelHeight"] = "1080";
            row["ThumbnailPixelWidth"] = "200";
            row["ThumbnailPixelHeight"] = "112";
            row["ThumbnailCreationDateTime"] = "25/07/2020 9:45:47";
            row["Description"] = "Second file description; Includes separator character escaped.";
            row["Hash"] = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124";
            table.Rows.Add(row);

            table.AcceptChanges();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = true },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = true },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = true }
                }
            });

            portableDatabase.WriteDataTable(table);

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            string content = File.ReadAllText(filePath);
            content.Should().Be(expected);
        }

        [Fact]
        public void WriteDataTable_SomeColumnsWithEscapedText()
        {
            string expected = "\"FolderId\";FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;\"Description\";Hash\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"First file description\";4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"Second file description; Includes separator character escaped.\";0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            DataTable table = new(tableName);
            table.Columns.Add("FolderId");
            table.Columns.Add("FileName");
            table.Columns.Add("FileSize");
            table.Columns.Add("ImageRotation");
            table.Columns.Add("PixelWidth");
            table.Columns.Add("PixelHeight");
            table.Columns.Add("ThumbnailPixelWidth");
            table.Columns.Add("ThumbnailPixelHeight");
            table.Columns.Add("ThumbnailCreationDateTime");
            table.Columns.Add("Description");
            table.Columns.Add("Hash");

            DataRow row = table.NewRow();
            row["FolderId"] = "876283c6-780e-4ad5-975c-be63044c087a";
            row["FileName"] = "20200720175810_3.jpg";
            row["FileSize"] = "363888";
            row["ImageRotation"] = "Rotate0";
            row["PixelWidth"] = "1920";
            row["PixelHeight"] = "1080";
            row["ThumbnailPixelWidth"] = "200";
            row["ThumbnailPixelHeight"] = "112";
            row["ThumbnailCreationDateTime"] = "25/07/2020 9:45:47";
            row["Description"] = "First file description";
            row["Hash"] = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4";
            table.Rows.Add(row);

            row = table.NewRow();
            row["FolderId"] = "876283c6-780e-4ad5-975c-be63044c087a";
            row["FileName"] = "20200720175816_3.jpg";
            row["FileSize"] = "343633";
            row["ImageRotation"] = "Rotate0";
            row["PixelWidth"] = "1920";
            row["PixelHeight"] = "1080";
            row["ThumbnailPixelWidth"] = "200";
            row["ThumbnailPixelHeight"] = "112";
            row["ThumbnailCreationDateTime"] = "25/07/2020 9:45:47";
            row["Description"] = "Second file description; Includes separator character escaped.";
            row["Hash"] = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124";
            table.Rows.Add(row);

            table.AcceptChanges();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true }
                }
            });

            portableDatabase.WriteDataTable(table);

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            string content = File.ReadAllText(filePath);
            content.Should().Be(expected);
        }

        [Fact]
        public void WriteObjectList_AllColumnsWithUnescapedText()
        {
            string expected = "FolderId;FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;Hash\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            List<TestRecord> list = new()
            {
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175810_3.jpg",
                    FileSize = "363888",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Hash = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4"
                },
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175816_3.jpg",
                    FileSize = "343633",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Hash = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124"
                }
            };

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId" },
                    new ColumnProperties { ColumnName = "FileName" },
                    new ColumnProperties { ColumnName = "FileSize" },
                    new ColumnProperties { ColumnName = "ImageRotation" },
                    new ColumnProperties { ColumnName = "PixelWidth" },
                    new ColumnProperties { ColumnName = "PixelHeight" },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth" },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight" },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime" },
                    new ColumnProperties { ColumnName = "Hash" }
                }
            });

            portableDatabase.WriteObjectList(list, tableName, (r, i) =>
            {
                return i switch
                {
                    0 => r.FolderId,
                    1 => r.FileName,
                    2 => r.FileSize,
                    3 => r.ImageRotation,
                    4 => r.PixelWidth,
                    5 => r.PixelHeight,
                    6 => r.ThumbnailPixelWidth,
                    7 => r.ThumbnailPixelHeight,
                    8 => r.ThumbnailCreationDateTime,
                    9 => r.Hash,
                    _ => throw new ArgumentOutOfRangeException(nameof(i))
                };
            });

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            string content = File.ReadAllText(filePath);
            content.Should().Be(expected);
        }

        [Fact]
        public void WriteObjectList_AllColumnsWithEscapedText()
        {
            string expected = "\"FolderId\";\"FileName\";\"FileSize\";\"ImageRotation\";\"PixelWidth\";\"PixelHeight\";\"ThumbnailPixelWidth\";\"ThumbnailPixelHeight\";\"ThumbnailCreationDateTime\";\"Description\";\"Hash\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175810_3.jpg\";\"363888\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"First file description\";\"4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175816_3.jpg\";\"343633\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"Second file description; Includes separator character escaped.\";\"0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\"\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            List<TestRecord> list = new()
            {
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175810_3.jpg",
                    FileSize = "363888",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "First file description",
                    Hash = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4"
                },
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175816_3.jpg",
                    FileSize = "343633",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "Second file description; Includes separator character escaped.",
                    Hash = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124"
                }
            };

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = true },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = true },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = true }
                }
            });

            portableDatabase.WriteObjectList(list, tableName, (r, i) =>
            {
                return i switch
                {
                    0 => r.FolderId,
                    1 => r.FileName,
                    2 => r.FileSize,
                    3 => r.ImageRotation,
                    4 => r.PixelWidth,
                    5 => r.PixelHeight,
                    6 => r.ThumbnailPixelWidth,
                    7 => r.ThumbnailPixelHeight,
                    8 => r.ThumbnailCreationDateTime,
                    9 => r.Description,
                    10 => r.Hash,
                    _ => throw new ArgumentOutOfRangeException(nameof(i))
                };
            });

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            string content = File.ReadAllText(filePath);
            content.Should().Be(expected);
        }

        [Fact]
        public void WriteObjectList_SomeColumnsWithEscapedText()
        {
            string expected = "\"FolderId\";FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;\"Description\";Hash\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"First file description\";4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"Second file description; Includes separator character escaped.\";0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            List<TestRecord> list = new()
            {
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175810_3.jpg",
                    FileSize = "363888",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "First file description",
                    Hash = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4"
                },
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175816_3.jpg",
                    FileSize = "343633",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "Second file description; Includes separator character escaped.",
                    Hash = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124"
                }
            };

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = false },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = false },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = false }
                }
            });

            portableDatabase.WriteObjectList(list, tableName, (r, i) =>
            {
                return i switch
                {
                    0 => r.FolderId,
                    1 => r.FileName,
                    2 => r.FileSize,
                    3 => r.ImageRotation,
                    4 => r.PixelWidth,
                    5 => r.PixelHeight,
                    6 => r.ThumbnailPixelWidth,
                    7 => r.ThumbnailPixelHeight,
                    8 => r.ThumbnailCreationDateTime,
                    9 => r.Description,
                    10 => r.Hash,
                    _ => throw new ArgumentOutOfRangeException(nameof(i))
                };
            });

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            string content = File.ReadAllText(filePath);
            content.Should().Be(expected);
        }

        [Fact]
        public void WriteObjectList_NullList()
        {
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            List<TestRecord> list = null;

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = true },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = true },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = true }
                }
            });

            var action = new Action(() =>
            {
                portableDatabase.WriteObjectList(list, tableName, (r, i) =>
                {
                    return i switch
                    {
                        0 => r.FolderId,
                        1 => r.FileName,
                        2 => r.FileSize,
                        3 => r.ImageRotation,
                        4 => r.PixelWidth,
                        5 => r.PixelHeight,
                        6 => r.ThumbnailPixelWidth,
                        7 => r.ThumbnailPixelHeight,
                        8 => r.ThumbnailCreationDateTime,
                        9 => r.Description,
                        10 => r.Hash,
                        _ => throw new ArgumentOutOfRangeException(nameof(i))
                    };
                });
            });

            action.Should().Throw<ArgumentNullException>();
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        public void WriteObjectList_InvalidTableName(string tableName)
        {
            List<TestRecord> list = new()
            {
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175810_3.jpg",
                    FileSize = "363888",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "First file description",
                    Hash = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4"
                },
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175816_3.jpg",
                    FileSize = "343633",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "Second file description; Includes separator character escaped.",
                    Hash = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124"
                }
            };

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = false },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = false },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = false }
                }
            });

            var action = new Action(() =>
            {
                portableDatabase.WriteObjectList(list, tableName, (r, i) =>
                {
                    return i switch
                    {
                        0 => r.FolderId,
                        1 => r.FileName,
                        2 => r.FileSize,
                        3 => r.ImageRotation,
                        4 => r.PixelWidth,
                        5 => r.PixelHeight,
                        6 => r.ThumbnailPixelWidth,
                        7 => r.ThumbnailPixelHeight,
                        8 => r.ThumbnailCreationDateTime,
                        9 => r.Description,
                        10 => r.Hash,
                        _ => throw new ArgumentOutOfRangeException(nameof(i))
                    };
                });
            });

            action.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void WriteObjectList_NoDataTableProperties()
        {
            string expected = "\"FolderId\";\"FileName\";\"FileSize\";\"ImageRotation\";\"PixelWidth\";\"PixelHeight\";\"ThumbnailPixelWidth\";\"ThumbnailPixelHeight\";\"ThumbnailCreationDateTime\";\"Description\";\"Hash\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175810_3.jpg\";\"363888\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"First file description\";\"4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175816_3.jpg\";\"343633\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"Second file description; Includes separator character escaped.\";\"0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\"\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            List<TestRecord> list = new()
            {
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175810_3.jpg",
                    FileSize = "363888",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "First file description",
                    Hash = "4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4"
                },
                new TestRecord
                {
                    FolderId = "876283c6-780e-4ad5-975c-be63044c087a",
                    FileName = "20200720175816_3.jpg",
                    FileSize = "343633",
                    ImageRotation = "Rotate0",
                    PixelWidth = "1920",
                    PixelHeight = "1080",
                    ThumbnailPixelWidth = "200",
                    ThumbnailPixelHeight = "112",
                    ThumbnailCreationDateTime = "25/07/2020 9:45:47",
                    Description = "Second file description; Includes separator character escaped.",
                    Hash = "0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124"
                }
            };

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = new(() =>
            {
                portableDatabase.WriteObjectList(list, tableName, (r, i) =>
                {
                    return i switch
                    {
                        0 => r.FolderId,
                        1 => r.FileName,
                        2 => r.FileSize,
                        3 => r.ImageRotation,
                        4 => r.PixelWidth,
                        5 => r.PixelHeight,
                        6 => r.ThumbnailPixelWidth,
                        7 => r.ThumbnailPixelHeight,
                        8 => r.ThumbnailCreationDateTime,
                        9 => r.Description,
                        10 => r.Hash,
                        _ => throw new ArgumentOutOfRangeException(nameof(i))
                    };
                });
            });

            action.Should().Throw<Exception>();
        }

        [Fact]
        public void ReadDataTable_AllColumnsWithUnescapedText()
        {
            string csv = "FolderId;FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;Hash\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = false },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = false },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = false }
                }
            });

            DataTable table = portableDatabase.ReadDataTable(tableName);
            
            table.Columns.Count.Should().Be(10);
            table.Columns[0].ColumnName.Should().Be("FolderId");
            table.Columns[1].ColumnName.Should().Be("FileName");
            table.Columns[2].ColumnName.Should().Be("FileSize");
            table.Columns[3].ColumnName.Should().Be("ImageRotation");
            table.Columns[4].ColumnName.Should().Be("PixelWidth");
            table.Columns[5].ColumnName.Should().Be("PixelHeight");
            table.Columns[6].ColumnName.Should().Be("ThumbnailPixelWidth");
            table.Columns[7].ColumnName.Should().Be("ThumbnailPixelHeight");
            table.Columns[8].ColumnName.Should().Be("ThumbnailCreationDateTime");
            table.Columns[9].ColumnName.Should().Be("Hash");

            table.TableName.Should().Be(tableName);
            table.Rows.Count.Should().Be(2);
            
            table.Rows[0]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[0]["FileName"].Should().Be("20200720175810_3.jpg");
            table.Rows[0]["FileSize"].Should().Be("363888");
            table.Rows[0]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[0]["PixelWidth"].Should().Be("1920");
            table.Rows[0]["PixelHeight"].Should().Be("1080");
            table.Rows[0]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[0]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[0]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[0]["Hash"].Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            table.Rows[1]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[1]["FileName"].Should().Be("20200720175816_3.jpg");
            table.Rows[1]["FileSize"].Should().Be("343633");
            table.Rows[1]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[1]["PixelWidth"].Should().Be("1920");
            table.Rows[1]["PixelHeight"].Should().Be("1080");
            table.Rows[1]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[1]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[1]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[1]["Hash"].Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadDataTable_AllColumnsWithUnescapedTextWithoutDataTableProperties()
        {
            string csv = "FolderId;FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;Hash\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            DataTable table = portableDatabase.ReadDataTable(tableName);

            table.Columns.Count.Should().Be(10);
            table.Columns[0].ColumnName.Should().Be("FolderId");
            table.Columns[1].ColumnName.Should().Be("FileName");
            table.Columns[2].ColumnName.Should().Be("FileSize");
            table.Columns[3].ColumnName.Should().Be("ImageRotation");
            table.Columns[4].ColumnName.Should().Be("PixelWidth");
            table.Columns[5].ColumnName.Should().Be("PixelHeight");
            table.Columns[6].ColumnName.Should().Be("ThumbnailPixelWidth");
            table.Columns[7].ColumnName.Should().Be("ThumbnailPixelHeight");
            table.Columns[8].ColumnName.Should().Be("ThumbnailCreationDateTime");
            table.Columns[9].ColumnName.Should().Be("Hash");

            table.TableName.Should().Be(tableName);
            table.Rows.Count.Should().Be(2);

            table.Rows[0]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[0]["FileName"].Should().Be("20200720175810_3.jpg");
            table.Rows[0]["FileSize"].Should().Be("363888");
            table.Rows[0]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[0]["PixelWidth"].Should().Be("1920");
            table.Rows[0]["PixelHeight"].Should().Be("1080");
            table.Rows[0]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[0]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[0]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[0]["Hash"].Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            table.Rows[1]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[1]["FileName"].Should().Be("20200720175816_3.jpg");
            table.Rows[1]["FileSize"].Should().Be("343633");
            table.Rows[1]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[1]["PixelWidth"].Should().Be("1920");
            table.Rows[1]["PixelHeight"].Should().Be("1080");
            table.Rows[1]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[1]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[1]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[1]["Hash"].Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadDataTable_AllColumnsWithEscapedText()
        {
            string csv = "\"FolderId\";\"FileName\";\"FileSize\";\"ImageRotation\";\"PixelWidth\";\"PixelHeight\";\"ThumbnailPixelWidth\";\"ThumbnailPixelHeight\";\"ThumbnailCreationDateTime\";\"Description\";\"Hash\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175810_3.jpg\";\"363888\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"First file description\";\"4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175816_3.jpg\";\"343633\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"Second file description; Includes separator character escaped.\";\"0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\"\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = true },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = true },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = true }
                }
            });

            DataTable table = portableDatabase.ReadDataTable(tableName);

            table.Columns.Count.Should().Be(11);
            table.Columns[0].ColumnName.Should().Be("FolderId");
            table.Columns[1].ColumnName.Should().Be("FileName");
            table.Columns[2].ColumnName.Should().Be("FileSize");
            table.Columns[3].ColumnName.Should().Be("ImageRotation");
            table.Columns[4].ColumnName.Should().Be("PixelWidth");
            table.Columns[5].ColumnName.Should().Be("PixelHeight");
            table.Columns[6].ColumnName.Should().Be("ThumbnailPixelWidth");
            table.Columns[7].ColumnName.Should().Be("ThumbnailPixelHeight");
            table.Columns[8].ColumnName.Should().Be("ThumbnailCreationDateTime");
            table.Columns[9].ColumnName.Should().Be("Description");
            table.Columns[10].ColumnName.Should().Be("Hash");

            table.TableName.Should().Be(tableName);
            table.Rows.Count.Should().Be(2);

            table.Rows[0]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[0]["FileName"].Should().Be("20200720175810_3.jpg");
            table.Rows[0]["FileSize"].Should().Be("363888");
            table.Rows[0]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[0]["PixelWidth"].Should().Be("1920");
            table.Rows[0]["PixelHeight"].Should().Be("1080");
            table.Rows[0]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[0]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[0]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[0]["Description"].Should().Be("First file description");
            table.Rows[0]["Hash"].Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            table.Rows[1]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[1]["FileName"].Should().Be("20200720175816_3.jpg");
            table.Rows[1]["FileSize"].Should().Be("343633");
            table.Rows[1]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[1]["PixelWidth"].Should().Be("1920");
            table.Rows[1]["PixelHeight"].Should().Be("1080");
            table.Rows[1]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[1]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[1]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[1]["Description"].Should().Be("Second file description; Includes separator character escaped.");
            table.Rows[1]["Hash"].Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadDataTable_SomeColumnsWithEscapedText()
        {
            string csv = "\"FolderId\";FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;\"Description\";Hash\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"First file description\";4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"Second file description; Includes separator character escaped.\";0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = false },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = false },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = false },
                }
            });

            DataTable table = portableDatabase.ReadDataTable(tableName);

            table.Columns.Count.Should().Be(11);
            table.Columns[0].ColumnName.Should().Be("FolderId");
            table.Columns[1].ColumnName.Should().Be("FileName");
            table.Columns[2].ColumnName.Should().Be("FileSize");
            table.Columns[3].ColumnName.Should().Be("ImageRotation");
            table.Columns[4].ColumnName.Should().Be("PixelWidth");
            table.Columns[5].ColumnName.Should().Be("PixelHeight");
            table.Columns[6].ColumnName.Should().Be("ThumbnailPixelWidth");
            table.Columns[7].ColumnName.Should().Be("ThumbnailPixelHeight");
            table.Columns[8].ColumnName.Should().Be("ThumbnailCreationDateTime");
            table.Columns[9].ColumnName.Should().Be("Description");
            table.Columns[10].ColumnName.Should().Be("Hash");

            table.TableName.Should().Be(tableName);
            table.Rows.Count.Should().Be(2);

            table.Rows[0]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[0]["FileName"].Should().Be("20200720175810_3.jpg");
            table.Rows[0]["FileSize"].Should().Be("363888");
            table.Rows[0]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[0]["PixelWidth"].Should().Be("1920");
            table.Rows[0]["PixelHeight"].Should().Be("1080");
            table.Rows[0]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[0]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[0]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[0]["Description"].Should().Be("First file description");
            table.Rows[0]["Hash"].Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            table.Rows[1]["FolderId"].Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            table.Rows[1]["FileName"].Should().Be("20200720175816_3.jpg");
            table.Rows[1]["FileSize"].Should().Be("343633");
            table.Rows[1]["ImageRotation"].Should().Be("Rotate0");
            table.Rows[1]["PixelWidth"].Should().Be("1920");
            table.Rows[1]["PixelHeight"].Should().Be("1080");
            table.Rows[1]["ThumbnailPixelWidth"].Should().Be("200");
            table.Rows[1]["ThumbnailPixelHeight"].Should().Be("112");
            table.Rows[1]["ThumbnailCreationDateTime"].Should().Be("25/07/2020 9:45:47");
            table.Rows[1]["Description"].Should().Be("Second file description; Includes separator character escaped.");
            table.Rows[1]["Hash"].Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadObjectList_AllColumnsWithEscapedText()
        {
            string csv = "\"FolderId\";\"FileName\";\"FileSize\";\"ImageRotation\";\"PixelWidth\";\"PixelHeight\";\"ThumbnailPixelWidth\";\"ThumbnailPixelHeight\";\"ThumbnailCreationDateTime\";\"Description\";\"Hash\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175810_3.jpg\";\"363888\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"First file description\";\"4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\"\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";\"20200720175816_3.jpg\";\"343633\";\"Rotate0\";\"1920\";\"1080\";\"200\";\"112\";\"25/07/2020 9:45:47\";\"Second file description; Includes separator character escaped.\";\"0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\"\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = true },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = true },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = true },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = true }
                }
            });

            List<TestRecord> list = portableDatabase.ReadObjectList(tableName, f =>
                new TestRecord
                {
                    FolderId = f[0],
                    FileName = f[1],
                    FileSize = f[2],
                    ImageRotation = f[3],
                    PixelWidth = f[4],
                    PixelHeight = f[5],
                    ThumbnailPixelWidth = f[6],
                    ThumbnailPixelHeight = f[7],
                    ThumbnailCreationDateTime = f[8],
                    Description = f[9],
                    Hash = f[10]
                });

            list.Should().HaveCount(2);

            list[0].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[0].FileName.Should().Be("20200720175810_3.jpg");
            list[0].FileSize.Should().Be("363888");
            list[0].ImageRotation.Should().Be("Rotate0");
            list[0].PixelWidth.Should().Be("1920");
            list[0].PixelHeight.Should().Be("1080");
            list[0].ThumbnailPixelWidth.Should().Be("200");
            list[0].ThumbnailPixelHeight.Should().Be("112");
            list[0].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[0].Description.Should().Be("First file description");
            list[0].Hash.Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            list[1].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[1].FileName.Should().Be("20200720175816_3.jpg");
            list[1].FileSize.Should().Be("343633");
            list[1].ImageRotation.Should().Be("Rotate0");
            list[1].PixelWidth.Should().Be("1920");
            list[1].PixelHeight.Should().Be("1080");
            list[1].ThumbnailPixelWidth.Should().Be("200");
            list[1].ThumbnailPixelHeight.Should().Be("112");
            list[1].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[1].Description.Should().Be("Second file description; Includes separator character escaped.");
            list[1].Hash.Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");
            
            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadObjectList_AllColumnsWithUnescapedText()
        {
            string csv = "FolderId;FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;Hash\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = false },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = false },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = false }
                }
            });

            List<TestRecord> list = portableDatabase.ReadObjectList(tableName, f =>
                new TestRecord
                {
                    FolderId = f[0],
                    FileName = f[1],
                    FileSize = f[2],
                    ImageRotation = f[3],
                    PixelWidth = f[4],
                    PixelHeight = f[5],
                    ThumbnailPixelWidth = f[6],
                    ThumbnailPixelHeight = f[7],
                    ThumbnailCreationDateTime = f[8],
                    Hash = f[9]
                });

            list.Should().HaveCount(2);

            list[0].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[0].FileName.Should().Be("20200720175810_3.jpg");
            list[0].FileSize.Should().Be("363888");
            list[0].ImageRotation.Should().Be("Rotate0");
            list[0].PixelWidth.Should().Be("1920");
            list[0].PixelHeight.Should().Be("1080");
            list[0].ThumbnailPixelWidth.Should().Be("200");
            list[0].ThumbnailPixelHeight.Should().Be("112");
            list[0].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[0].Hash.Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            list[1].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[1].FileName.Should().Be("20200720175816_3.jpg");
            list[1].FileSize.Should().Be("343633");
            list[1].ImageRotation.Should().Be("Rotate0");
            list[1].PixelWidth.Should().Be("1920");
            list[1].PixelHeight.Should().Be("1080");
            list[1].ThumbnailPixelWidth.Should().Be("200");
            list[1].ThumbnailPixelHeight.Should().Be("112");
            list[1].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[1].Hash.Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadObjectList_AllColumnsWithUnescapedTextWithoutDataTableProperties()
        {
            string csv = "FolderId;FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;Hash\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "876283c6-780e-4ad5-975c-be63044c087a;20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            var list = portableDatabase.ReadObjectList(tableName, f =>
                new TestRecord
                {
                    FolderId = f[0],
                    FileName = f[1],
                    FileSize = f[2],
                    ImageRotation = f[3],
                    PixelWidth = f[4],
                    PixelHeight = f[5],
                    ThumbnailPixelWidth = f[6],
                    ThumbnailPixelHeight = f[7],
                    ThumbnailCreationDateTime = f[8],
                    Hash = f[9]
                });

            list.Should().HaveCount(2);

            list[0].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[0].FileName.Should().Be("20200720175810_3.jpg");
            list[0].FileSize.Should().Be("363888");
            list[0].ImageRotation.Should().Be("Rotate0");
            list[0].PixelWidth.Should().Be("1920");
            list[0].PixelHeight.Should().Be("1080");
            list[0].ThumbnailPixelWidth.Should().Be("200");
            list[0].ThumbnailPixelHeight.Should().Be("112");
            list[0].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[0].Hash.Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            list[1].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[1].FileName.Should().Be("20200720175816_3.jpg");
            list[1].FileSize.Should().Be("343633");
            list[1].ImageRotation.Should().Be("Rotate0");
            list[1].PixelWidth.Should().Be("1920");
            list[1].PixelHeight.Should().Be("1080");
            list[1].ThumbnailPixelWidth.Should().Be("200");
            list[1].ThumbnailPixelHeight.Should().Be("112");
            list[1].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[1].Hash.Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void ReadObjectList_SomeColumnsWithEscapedText()
        {
            string csv = "\"FolderId\";FileName;FileSize;ImageRotation;PixelWidth;PixelHeight;ThumbnailPixelWidth;ThumbnailPixelHeight;ThumbnailCreationDateTime;\"Description\";Hash\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175810_3.jpg;363888;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"First file description\";4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4\r\n" +
                "\"876283c6-780e-4ad5-975c-be63044c087a\";20200720175816_3.jpg;343633;Rotate0;1920;1080;200;112;25/07/2020 9:45:47;\"Second file description; Includes separator character escaped.\";0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124\r\n";
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            File.WriteAllText(filePath, csv);

            portableDatabase.SetDataTableProperties(new DataTableProperties
            {
                TableName = tableName,
                ColumnProperties = new ColumnProperties[]
                {
                    new ColumnProperties { ColumnName = "FolderId", EscapeText = true },
                    new ColumnProperties { ColumnName = "FileName", EscapeText = false },
                    new ColumnProperties { ColumnName = "FileSize", EscapeText = false },
                    new ColumnProperties { ColumnName = "ImageRotation", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "PixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelWidth", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailPixelHeight", EscapeText = false },
                    new ColumnProperties { ColumnName = "ThumbnailCreationDateTime", EscapeText = false },
                    new ColumnProperties { ColumnName = "Description", EscapeText = true },
                    new ColumnProperties { ColumnName = "Hash", EscapeText = false },
                }
            });

            List<TestRecord> list = portableDatabase.ReadObjectList(tableName, f =>
                new TestRecord
                {
                    FolderId = f[0],
                    FileName = f[1],
                    FileSize = f[2],
                    ImageRotation = f[3],
                    PixelWidth = f[4],
                    PixelHeight = f[5],
                    ThumbnailPixelWidth = f[6],
                    ThumbnailPixelHeight = f[7],
                    ThumbnailCreationDateTime = f[8],
                    Description = f[9],
                    Hash = f[10]
                });

            list.Should().HaveCount(2);

            list[0].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[0].FileName.Should().Be("20200720175810_3.jpg");
            list[0].FileSize.Should().Be("363888");
            list[0].ImageRotation.Should().Be("Rotate0");
            list[0].PixelWidth.Should().Be("1920");
            list[0].PixelHeight.Should().Be("1080");
            list[0].ThumbnailPixelWidth.Should().Be("200");
            list[0].ThumbnailPixelHeight.Should().Be("112");
            list[0].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[0].Description.Should().Be("First file description");
            list[0].Hash.Should().Be("4e50d5c7f1a64b5d61422382ac822641ad4e5b943aca9ade955f4655f799558bb0ae9c342ee3ead0949b32019b25606bd16988381108f56bb6c6dd673edaa1e4");

            list[1].FolderId.Should().Be("876283c6-780e-4ad5-975c-be63044c087a");
            list[1].FileName.Should().Be("20200720175816_3.jpg");
            list[1].FileSize.Should().Be("343633");
            list[1].ImageRotation.Should().Be("Rotate0");
            list[1].PixelWidth.Should().Be("1920");
            list[1].PixelHeight.Should().Be("1080");
            list[1].ThumbnailPixelWidth.Should().Be("200");
            list[1].ThumbnailPixelHeight.Should().Be("112");
            list[1].ThumbnailCreationDateTime.Should().Be("25/07/2020 9:45:47");
            list[1].Description.Should().Be("Second file description; Includes separator character escaped.");
            list[1].Hash.Should().Be("0af8f118b7d606e5d174643727bd3c0c6028b52c50481585274fd572110b108c7a0d7901227f75a72b44c89335e002a65e8137ff5b238ab1c0bba0505e783124");

            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastReadFileRaw.Should().Be(csv);
        }

        [Fact]
        public void SetDataTableProperties_NullDataTableProperties_ThrowException()
        {
            DataTable table = new();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () =>
                portableDatabase.SetDataTableProperties(null);

            action.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void SetDataTableProperties_NoColumnsProperties_ThrowException()
        {
            DataTable table = new();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () =>
                portableDatabase.SetDataTableProperties(new DataTableProperties());

            action.Should().Throw<ArgumentException>();
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void SetDataTableProperties_ColumnPropertiesWithInvalidColumnName_ThrowException(string columnName)
        {
            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () =>
                portableDatabase.SetDataTableProperties(new DataTableProperties
                {
                    TableName = "TableA",
                    ColumnProperties = new ColumnProperties[]
                    {
                        new ColumnProperties { ColumnName = columnName, EscapeText = true }
                    }
                });

            action.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void SetDataTableProperties_DuplicateColumnProperties_ThrowException()
        {
            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () =>
                portableDatabase.SetDataTableProperties(new DataTableProperties
                {
                    TableName = "TableA",
                    ColumnProperties = new ColumnProperties[]
                    {
                        new ColumnProperties { ColumnName = "ColumnA", EscapeText = true },
                        new ColumnProperties { ColumnName = "ColumnB", EscapeText = true },
                        new ColumnProperties { ColumnName = "ColumnB", EscapeText = true }
                    }
                });

            action.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void WriteDataTable_NullDataTable_ThrowException()
        {
            DataTable table = new();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () => portableDatabase.WriteDataTable(null);
            action.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void WriteDataTable_DataTableWithNoTableName_ThrowException()
        {
            DataTable table = new();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () => portableDatabase.WriteDataTable(new DataTable());
            action.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void WriteDataTable_DataTableWithNoColumns_ThrowException()
        {
            DataTable table = new();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () => portableDatabase.WriteDataTable(new DataTable("Data"));
            action.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void WriteDataTable_DataColumnWithNoName_ThrowException()
        {
            string tableName = "assets" + Guid.NewGuid();
            string filePath = Path.Combine("TestData", "Tables", tableName + ".db");

            DataTable table = new(tableName);
            table.Columns.Add("FolderId");
            table.Columns.Add(" ");
            
            table.AcceptChanges();

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            Action action = () => portableDatabase.WriteDataTable(table);
            action.Should().Throw<ArgumentException>();
        }

        [Theory]
        [InlineData("utf-8", "Hello world")]
        [InlineData("utf-32", "Hello world")]
        [InlineData("us-ascii", "Hello world")]
        [InlineData("utf-8", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        [InlineData("utf-32", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        [InlineData("us-ascii", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        public void WriteBlob(string encodingName, string message)
        {
            byte[] expected = Encoding.GetEncoding(encodingName).GetBytes(message);
            string blobName = "blob" + Guid.NewGuid() + ".bin";
            string filePath = Path.Combine("TestData", "Blobs", blobName);

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.WriteBlob(expected, blobName);

            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);
            portableDatabase.Diagnostics.LastWriteFileRaw.Should().Be(expected);

            byte[] blob;

            using (FileStream fileStream = new(filePath, FileMode.Open))
            {
                BinaryFormatter binaryFormatter = new();
                blob = (byte[])binaryFormatter.Deserialize(fileStream);
            }

            blob.Should().HaveSameCount(expected);
            blob.Should().ContainInOrder(expected);
        }

        [Theory]
        [InlineData("utf-8", "Hello world")]
        [InlineData("utf-32", "Hello world")]
        [InlineData("us-ascii", "Hello world")]
        [InlineData("utf-8", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        [InlineData("utf-32", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        [InlineData("us-ascii", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        public void ReadBlob(string encodingName, string message)
        {
            byte[] expected = Encoding.GetEncoding(encodingName).GetBytes(message);
            string blobName = "blob" + Guid.NewGuid() + ".bin";
            string filePath = Path.Combine("TestData", "Blobs", blobName);

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');

            using (FileStream fileStream = new(filePath, FileMode.Create))
            {
                BinaryFormatter binaryFormatter = new();
                binaryFormatter.Serialize(fileStream, expected);
            }

            byte[] blob = (byte[])portableDatabase.ReadBlob(blobName);
            portableDatabase.Diagnostics.LastReadFilePath.Should().Be(filePath);
            
            blob.Should().HaveSameCount(expected);
            blob.Should().ContainInOrder(expected);
        }

        [Theory]
        [InlineData("utf-8", "Hello world")]
        [InlineData("utf-32", "Hello world")]
        [InlineData("us-ascii", "Hello world")]
        [InlineData("utf-8", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        [InlineData("utf-32", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        [InlineData("us-ascii", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")]
        public void ReadBlob_SameAsInputForWriteBlob(string encodingName, string message)
        {
            byte[] expected = Encoding.GetEncoding(encodingName).GetBytes(message);
            string blobName = "blob" + Guid.NewGuid() + ".bin";

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.WriteBlob(expected, blobName);
            byte[] blob = (byte[])portableDatabase.ReadBlob(blobName);
            
            blob.Should().HaveSameCount(expected);
            blob.Should().ContainInOrder(expected);
        }

        [Fact]
        public void WriteBackup()
        {
            string backupName = "20220108.zip";
            string filePath = Path.Combine("TestData_Backups", backupName);

            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                new BackupStorage());
            portableDatabase.Initialize("TestData", ';');
            bool result = portableDatabase.WriteBackup(new DateTime(2022, 1, 8).Date);

            result.Should().BeTrue();
            portableDatabase.Diagnostics.LastWriteFilePath.Should().Be(filePath);

            ZipFile.ExtractToDirectory(filePath, "TestData_Backups_Test");

            var sourceDirectories = Directory.GetDirectories("TestData");
            var backupDirectories = Directory.GetDirectories("TestData_Backups_Test");

            sourceDirectories.Should().HaveSameCount(backupDirectories);
            sourceDirectories[0].Should().Be(@"TestData\Blobs");
            sourceDirectories[1].Should().Be(@"TestData\Tables");
            backupDirectories[0].Should().Be(@"TestData_Backups_Test\Blobs");
            backupDirectories[1].Should().Be(@"TestData_Backups_Test\Tables");
        }

        [Fact]
        public void DeleteOldBackups()
        {
            string backupName = "20220108.zip";
            string filePath = Path.Combine("TestData_Backups", backupName);

            using var mock = AutoMock.GetLoose();

            mock.Mock<IBackupStorage>()
                .Setup(b => b.GetBackupFiles(It.IsAny<string>()))
                .Returns(new[]
                {
                    @"TestData_Backups\20220104.zip",
                    @"TestData_Backups\20220105.zip",
                    @"TestData_Backups\20220107.zip",
                    @"TestData_Backups\20220106.zip",
                    @"TestData_Backups\20220108.zip"
                });

            IBackupStorage backupStorage = mock.Mock<IBackupStorage>().Object;
            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                backupStorage);
            portableDatabase.Initialize("TestData", ';');
            portableDatabase.DeleteOldBackups(2);

            portableDatabase.Diagnostics.LastDeletedBackupFilePaths.Should().HaveCount(3);
            portableDatabase.Diagnostics.LastDeletedBackupFilePaths.Should().Contain(@"TestData_Backups\20220104.zip");
            portableDatabase.Diagnostics.LastDeletedBackupFilePaths.Should().Contain(@"TestData_Backups\20220105.zip");
            portableDatabase.Diagnostics.LastDeletedBackupFilePaths.Should().Contain(@"TestData_Backups\20220106.zip");

            mock.Mock<IBackupStorage>().Verify(b => b.DeleteBackupFile(@"TestData_Backups\20220104.zip"), Times.Once);
            mock.Mock<IBackupStorage>().Verify(b => b.DeleteBackupFile(@"TestData_Backups\20220105.zip"), Times.Once);
            mock.Mock<IBackupStorage>().Verify(b => b.DeleteBackupFile(@"TestData_Backups\20220106.zip"), Times.Once);
            mock.Mock<IBackupStorage>().Verify(b => b.DeleteBackupFile(@"TestData_Backups\20220107.zip"), Times.Never);
            mock.Mock<IBackupStorage>().Verify(b => b.DeleteBackupFile(@"TestData_Backups\20220108.zip"), Times.Never);
        }

        [Fact]
        public void GetBackupDates()
        {
            string backupName = "20220108.zip";
            string filePath = Path.Combine("TestData_Backups", backupName);

            using var mock = AutoMock.GetLoose();

            mock.Mock<IBackupStorage>()
                .Setup(b => b.GetBackupFiles(It.IsAny<string>()))
                .Returns(new[]
                {
                    @"TestData_Backups\20220104.zip",
                    @"TestData_Backups\20220105.zip",
                    @"TestData_Backups\20220107.zip",
                    @"TestData_Backups\20220106.zip",
                    @"TestData_Backups\20220108.zip"
                });

            IBackupStorage backupStorage = mock.Mock<IBackupStorage>().Object;
            IDatabase portableDatabase = new Database(new ObjectListStorage(),
                new DataTableStorage(),
                new BlobStorage(),
                backupStorage);
            portableDatabase.Initialize("TestData", ';');
            var backupDates = portableDatabase.GetBackupDates();
            backupDates.Should().HaveCount(5);
            backupDates[0].Date.Should().Be(new DateTime(2022, 1, 4));
            backupDates[1].Date.Should().Be(new DateTime(2022, 1, 5));
            backupDates[2].Date.Should().Be(new DateTime(2022, 1, 6));
            backupDates[3].Date.Should().Be(new DateTime(2022, 1, 7));
            backupDates[4].Date.Should().Be(new DateTime(2022, 1, 8));
        }
    }

    public class TestRecord
    {
        public string FolderId { get; set; }
        public string FileName { get; set; }
        public string FileSize { get; set; }
        public string ImageRotation { get; set; }
        public string PixelWidth { get; set; }
        public string PixelHeight { get; set; }
        public string ThumbnailPixelWidth { get; set; }
        public string ThumbnailPixelHeight { get; set; }
        public string ThumbnailCreationDateTime { get; set; }
        public string Description { get; set; }
        public string Hash { get; set; }
    }
}
