/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Alat Logging
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ILogger {
        string LogInfoFolderPath { get; }
        string LogErrorFolderPath { get; }
        void SetLogReporter(IProgress<string> infoReporter);
        void WriteInfo(string subject, string body, bool newLine = false, bool force = false);
        void WriteError(string errorMessage, int skipFrame = 1);
        void WriteError(Exception errorException, int skipFrame = 2);
    }

    public sealed class CLogger : ILogger {

        private readonly IApplication _app;

        public string LogInfoFolderPath { get; }

        public string LogErrorFolderPath { get; }

        public IProgress<string> LogReporter = null;

        private string sSw = null;
        private StreamWriter swInfo = null;
        private StreamWriter swError = null;

        public CLogger(IApplication app) {
            this._app = app;

            this.LogInfoFolderPath = Path.Combine(this._app.AppLocation, "_data", "Info_Logs");
            if (!Directory.Exists(this.LogInfoFolderPath)) {
                _ = Directory.CreateDirectory(this.LogInfoFolderPath);
            }

            this.LogErrorFolderPath = Path.Combine(this._app.AppLocation, "_data", "Error_Logs");
            if (!Directory.Exists(this.LogErrorFolderPath)) {
                _ = Directory.CreateDirectory(this.LogErrorFolderPath);
            }

            this.CheckStreamWritter();
        }

        private void CheckStreamWritter() {
            if (this.sSw != $"{DateTime.Now:yyyy-MM-dd}") {
                this.sSw = $"{DateTime.Now:yyyy-MM-dd}";

                if (this.swInfo != null) {
                    this.swInfo.Dispose();
                }

                this.swInfo = new StreamWriter($"{this.LogInfoFolderPath}/{this.sSw}.log", true);

                if (this.swError != null) {
                    this.swError.Dispose();
                }

                this.swError = new StreamWriter($"{this.LogErrorFolderPath}/{this.sSw}.log", true);
            }
        }

        public void SetLogReporter(IProgress<string> logReporter) {
            this.LogReporter = logReporter;
        }

        public void WriteInfo(string subject, string body, bool newLine = false, bool force = false) {
            try {
                string threadName = Thread.CurrentThread.Name ?? string.Empty;
                this.CheckStreamWritter();

                if (!string.IsNullOrEmpty(threadName)) {
                    threadName += " :: ";
                }

                threadName += $"{DateTime.Now:HH:mm:ss tt zzz}";

                string content = $"[{threadName}] {subject} :: {body} {Environment.NewLine}";
                if (newLine) {
                    content += Environment.NewLine;
                }

                if (this.LogReporter != null) {
                    this.LogReporter.Report(content);
                }

                if (this._app.DebugMode || force) {
                    this.swInfo.WriteLine(content);
                    this.swInfo.Flush();
                }
            }
            catch (Exception ex) {
                this.WriteError(ex);
            }
        }

        public void WriteError(string errorMessage, int skipFrame = 1) {
            try {
                this.CheckStreamWritter();
                var fromsub = new StackFrame(skipFrame, false);
                string content = $"##" + Environment.NewLine;
                content += $"#  ErrDate : {DateTime.Now:dd-MM-yyyy HH:mm:ss}" + Environment.NewLine;
                content += $"#  ErrFunc : {fromsub.GetMethod().Name}" + Environment.NewLine;
                content += $"#  ErrInfo : {errorMessage}" + Environment.NewLine;
                content += $"##" + Environment.NewLine;
                if (this.LogReporter != null) {
                    this.LogReporter.Report(content);
                }

                this.swError.WriteLine(content);
                this.swError.Flush();
            }
            catch (Exception ex) {
                // Nyerah ~~
                // Re-Throw Lempar Ke Main Thread (?)
                // Mungkin Akan Membuat Force-Close Program
                // Comment Jika Mau Beneran Menyerah Dan Gak Ngapa"in
                // Agar Tidak Force-Close Dan Tetap Jalan
                throw ex;
            }
        }

        public void WriteError(Exception errorException, int skipFrame = 2) {
            string errMsg = errorException.Message;

            Exception innerException = errorException.InnerException;
            while (innerException != null) {
                errMsg += " ~ " + innerException.Message;
                innerException = innerException.InnerException;
            }

            this.WriteError(errMsg, skipFrame);
        }

    }

}
