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
        void DataTable2CSV(DataTable dt, string filename, string separator, string outputPath = null);
        List<T> Csv2List<T>(Stream stream, char delimiter = ',', bool skipHeader = false, List<string> csvColumn = null, List<string> requiredColumn = null);
        string Csv2Json(string filePath, string delimiter, List<CCsv2Json> csvColumn = null);
    }

    public sealed class CCsv : ICsv {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;

        public string CsvFolderPath { get; }

        public CCsv(IApplication app, ILogger logger, IConfig config) {
            this._app = app;
            this._logger = logger;
            this._config = config;

            this.CsvFolderPath = this._config.Get<string>("CsvFolderPath", Path.Combine(this._app.AppLocation, "_data", "Csv_Files"));
            if (!Directory.Exists(this.CsvFolderPath)) {
                Directory.CreateDirectory(this.CsvFolderPath);
            }
        }

        public void DataTable2CSV(DataTable dt, string filename, string separator, string outputPath = null) {
            try {
                string path = Path.Combine(outputPath ?? this.CsvFolderPath, filename);
                dt.ToCsv(separator, path);
                this._logger.WriteInfo($"{this.GetType().Name}Dt2Csv", path);
            }
            catch (Exception ex) {
                this._logger.WriteError(ex.Message);
                throw ex;
            }
        }

        public List<T> Csv2List<T>(Stream stream, char delimiter = ',', bool skipHeader = false, List<string> csvColumn = null, List<string> requiredColumn = null) {
            using (var reader = new StreamReader(stream)) {
                int i = 0;
                List<string> col = csvColumn ?? new List<string>();
                var row = new List<T>();

                if (skipHeader && csvColumn != null) {
                    i++;
                    reader.ReadLine();
                }

                while (!reader.EndOfStream) {
                    string line = reader.ReadLine();
                    if (!string.IsNullOrEmpty(line)) {
                        string[] values = line.Split(delimiter).Select(v => v.StartsWith("\"") && v.EndsWith("\"") ? v.Substring(1, v.Length - 2) : v).ToArray();

                        if (i == 0) {
                            if (csvColumn == null) {
                                col.AddRange(values);
                            }
                            else {
                                var temp = new List<string>();
                                for (int j = 0; j < values.Length; j++) {
                                    if (csvColumn.Select(ac => ac.ToUpper()).Contains(values[j].ToUpper())) {
                                        temp.Add(values[j]);
                                    }
                                }

                                if (temp.Count != col.Count) {
                                    throw new Exception("Data kolom yang tersedia tidak lengkap");
                                }

                                col = temp;
                            }
                        }
                        else if (values.Length != col.Count) {
                            throw new Exception("Jumlah kolom data tidak sesuai dengan kolom header");
                        }
                        else {
                            PropertyInfo[] properties = typeof(T).GetProperties();
                            T objT = Activator.CreateInstance<T>();

                            for (int j = 0; j < col.Count; j++) {
                                string colName = col[j].ToUpper();
                                string rowVal = values[j].ToUpper();

                                if (csvColumn != null && requiredColumn != null) {
                                    if (requiredColumn.Select(rc => rc.ToUpper()).Contains(colName)) {
                                        if (string.IsNullOrEmpty(rowVal)) {
                                            throw new Exception($"Baris {i + 1} kolom {j} :: {colName} tidak boleh kosong");
                                        }
                                    }
                                }

                                foreach (PropertyInfo pro in properties) {
                                    if (pro.Name.ToUpper() == colName) {
                                        try {
                                            pro.SetValue(objT, rowVal);
                                        }
                                        catch {
                                            //
                                        }
                                    }
                                }
                            }

                            row.Add(objT);
                        }
                    }

                    i++;
                }

                return row;
            }
        }

        public string Csv2Json(string filePath, string delimiter, List<CCsv2Json> csvColumn = null) {
            if (csvColumn == null || csvColumn?.Count <= 0) {
                throw new Exception("Daftar Kolom Harus Di Isi");
            }

            var sb = new StringBuilder();
            using (ChoCSVReader<dynamic> csv = new ChoCSVReader(filePath).WithDelimiter(delimiter)) {
                ChoCSVReader<dynamic> _data = csv;

                if (csvColumn != null) {
                    for (int i = 0; i < csvColumn.Count; i++) {
                        int pos = csvColumn[i].Position;
                        _data = _data.WithField(csvColumn[i].ColumnName, (pos > 0) ? pos : (i + 1), csvColumn[i].DataType);
                    }
                    _data = _data.WithFirstLineHeader(true);
                }

                using (var w = new ChoJSONWriter(sb)) {
                    w.Write(csv);
                }
            }

            return sb.ToString();
        }

    }

}
