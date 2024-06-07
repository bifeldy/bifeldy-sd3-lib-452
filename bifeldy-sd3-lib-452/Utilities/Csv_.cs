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
using System.Data;
using System.IO;
using System.Text;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ICsv {
        string CsvFolderPath { get; }
        bool DataTable2CSV(DataTable table, string filename, string separator, string outputPath = null);
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

        public bool DataTable2CSV(DataTable table, string filename, string separator, string outputPath = null) {
            bool res = false;
            string path = Path.Combine(outputPath ?? this.CsvFolderPath, filename);
            StreamWriter writer = null;
            try {
                writer = new StreamWriter(path);
                string sep = string.Empty;
                var builder = new StringBuilder();
                foreach (DataColumn col in table.Columns) {
                    builder.Append(sep).Append(col.ColumnName);
                    sep = separator;
                }
                // Untuk Export *.CSV Di Buat NAMA_KOLOM Besar Semua Tanpa Petik "NAMA_KOLOM"
                writer.WriteLine(builder.ToString().ToUpper());
                foreach (DataRow row in table.Rows) {
                    sep = string.Empty;
                    builder = new StringBuilder();
                    foreach (DataColumn col in table.Columns) {
                        builder.Append(sep).Append(row[col.ColumnName]);
                        sep = separator;
                    }

                    writer.WriteLine(builder.ToString());
                }

                this._logger.WriteInfo($"{this.GetType().Name}Dt2Csv", path);
                res = true;
            }
            catch (Exception ex) {
                this._logger.WriteError(ex.Message);
            }
            finally {
                if (writer != null) {
                    writer.Close();
                }
            }

            return res;
        }

    }

}
