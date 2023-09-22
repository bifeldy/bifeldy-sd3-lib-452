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
using System.ComponentModel;
using System.Diagnostics;
using System.IO;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ILogger {
        string LogInfoFolderPath { get; }
        string LogErrorFolderPath { get; }
        void SetReportInfo(IProgress<string> infoReporter);
        void WriteInfo(string subject, string body, bool newLine = false);
        void WriteError(string errorMessage, int skipFrame = 1);
        void WriteError(Exception errorException, int skipFrame = 2);
    }

    public sealed class CLogger : ILogger {

        private readonly IApplication _app;

        public string LogInfoFolderPath { get; }

        public string LogErrorFolderPath { get; }

        public IProgress<string> LogInfoReporter = null;

        public CLogger(IApplication app) {
            _app = app;

            LogInfoFolderPath = Path.Combine(_app.AppLocation, "_data", "Info_Logs");
            if (!Directory.Exists(LogInfoFolderPath)) {
                Directory.CreateDirectory(LogInfoFolderPath);
            }
            LogErrorFolderPath = Path.Combine(_app.AppLocation, "_data", "Error_Logs");
            if (!Directory.Exists(LogErrorFolderPath)) {
                Directory.CreateDirectory(LogErrorFolderPath);
            }
        }

        public void SetReportInfo(IProgress<string> infoReporter) {
            LogInfoReporter = infoReporter;
        }

        public void WriteInfo(string subject, string body, bool newLine = false) {
            try {
                string content = $"[{DateTime.Now:HH:mm:ss tt zzz}] {subject} :: {body} {Environment.NewLine}";
                if (newLine) {
                    content += Environment.NewLine;
                }
                if (LogInfoReporter != null) {
                    LogInfoReporter.Report(content);
                }
                StreamWriter sw = new StreamWriter($"{LogInfoFolderPath}/{DateTime.Now:yyyy-MM-dd}.log", true);
                sw.WriteLine(content);
                sw.Flush();
                sw.Close();
            }
            catch (Exception ex) {
                WriteError(ex);
            }
        }

        public void WriteError(string errorMessage, int skipFrame = 1) {
            try {
                StackFrame fromsub = new StackFrame(skipFrame, false);
                StreamWriter sw = new StreamWriter($"{LogErrorFolderPath}/{DateTime.Now:yyyy-MM-dd}.log", true);
                sw.WriteLine($"##");
                sw.WriteLine($"#  ErrDate : {DateTime.Now:dd-MM-yyyy HH:mm:ss}");
                sw.WriteLine($"#  ErrFunc : {fromsub.GetMethod().Name}");
                sw.WriteLine($"#  ErrInfo : {errorMessage}");
                sw.WriteLine($"##");
                sw.Flush();
                sw.Close();
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
            WriteError(errorException.Message, skipFrame);
        }

    }

}
