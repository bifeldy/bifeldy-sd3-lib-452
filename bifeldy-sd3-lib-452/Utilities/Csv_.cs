/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: CSV Files Manager
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

using ChoETL;

using bifeldy_sd3_lib_452.Extensions;
using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ICsv {
        string CsvFolderPath { get; }
        DataTable Csv2DataTable(string filePath, string delimiter, List<CCsvColumn> csvColumn = null, string tableName = null);
        string Csv2Json(string filePath, string delimiter, List<CCsvColumn> csvColumn = null);
        List<T> Csv2List<T>(string filePath, string delimiter = ",", List<CCsvColumn> csvColumn = null);
    }

    public sealed class CCsv : ICsv {

        private readonly IApplication _app;
        private readonly IConfig _config;

        public string CsvFolderPath { get; }

        public CCsv(IApplication app, IConfig config) {
            this._app = app;
            this._config = config;

            this.CsvFolderPath = this._config.Get<string>("CsvFolderPath", Path.Combine(this._app.AppLocation, "_data", "Csv_Files"));
            if (!Directory.Exists(this.CsvFolderPath)) {
                Directory.CreateDirectory(this.CsvFolderPath);
            }
        }

        private ChoCSVReader<dynamic> ChoEtlSetupCsv(string filePath, string delimiter, List<CCsvColumn> csvColumn) {
            if (csvColumn == null || csvColumn?.Count <= 0) {
                throw new Exception("Daftar Kolom Harus Di Isi");
            }

            ChoCSVReader<dynamic> csv = new ChoCSVReader(filePath).WithDelimiter(delimiter);
            foreach (CCsvColumn cc in csvColumn) {
                csv = csv.WithField(cc.ColumnName, cc.Position, cc.DataType);
            }

            csv = csv.WithFirstLineHeader(true).MayHaveQuotedFields().MayContainEOLInData();
            return csv;
        }

        public DataTable Csv2DataTable(string filePath, string delimiter, List<CCsvColumn> csvColumn, string tableName = null) {
            csvColumn = csvColumn.OrderBy(c => c.Position).ToList();
            var fi = new FileInfo(filePath);

            using (ChoCSVReader<dynamic> csv = this.ChoEtlSetupCsv(filePath, delimiter, csvColumn)) {
                return csv.AsDataTable(tableName ?? fi.Name);
            }
        }

        public string Csv2Json(string filePath, string delimiter, List<CCsvColumn> csvColumn) {
            csvColumn = csvColumn.OrderBy(c => c.Position).ToList();
            var sb = new StringBuilder();

            using (ChoCSVReader<dynamic> csv = this.ChoEtlSetupCsv(filePath, delimiter, csvColumn)) {
                using (var w = new ChoJSONWriter(sb)) {
                    w.Write(csv);
                }
            }

            return sb.ToString();
        }

        public List<T> Csv2List<T>(string filePath, string delimiter, List<CCsvColumn> csvColumn) {
            csvColumn = csvColumn.OrderBy(c => c.Position).ToList();
            using (ChoCSVReader<dynamic> csv = this.ChoEtlSetupCsv(filePath, delimiter, csvColumn)) {
                using (IDataReader rdr = csv.AsDataReader()) {
                    DataColumnCollection columns = rdr.GetSchemaTable().Columns;
                    PropertyInfo[] properties = typeof(T).GetProperties();
                    var row = new List<T>();

                    while (rdr.Read()) {
                        T objT = Activator.CreateInstance<T>();
                        foreach (DataColumn column in columns) {
                            foreach (PropertyInfo pro in properties) {
                                if (pro.Name.ToUpper() == column.ColumnName.ToUpper()) {
                                    try {
                                        pro.SetValue(objT, rdr[column.ColumnName]);
                                    }
                                    catch {
                                        //
                                    }
                                }
                            }
                        }

                        row.Add(objT);
                    }

                    return row;
                }
            }
        }

    }

}
