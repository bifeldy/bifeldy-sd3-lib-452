/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
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
        string LogErrorFolderPath { get; }
        void SetReportLogInfoTarget(object control);
        void WriteLog(string subject, string body, bool newLine = false);
        void WriteError(string errorMessage, int skipFrame = 1);
        void WriteError(Exception errorException, int skipFrame = 2);
    }

    public sealed class CLogger : ILogger {

        private readonly IApplication _app;

        public string LogErrorFolderPath { get; }

        public IProgress<string> LogInformation = null;

        public CLogger(IApplication app) {
            _app = app;

            LogErrorFolderPath = Path.Combine(_app.AppLocation, "Error_Logs");
            if (!Directory.Exists(LogErrorFolderPath)) {
                Directory.CreateDirectory(LogErrorFolderPath);
            }
        }

        public void SetReportLogInfoTarget(dynamic control) {
            LogInformation = new Progress<string>(log => {
                control.Text += log;
            });
        }

        public void WriteLog(string subject, string body, bool newLine = false) {
            string content = $"[{DateTime.Now:HH:mm:ss tt zzz}] {subject} :: {body} {Environment.NewLine}";
            if (newLine) {
                content += Environment.NewLine;
            }
            if (LogInformation != null) {
                LogInformation.Report(content);
            }
        }

        public void WriteError(string errorMessage, int skipFrame = 1) {
            StackFrame fromsub = new StackFrame(skipFrame, false);
            StreamWriter sw = new StreamWriter($"{LogErrorFolderPath}/{DateTime.Now:dd-MM-yyyy}.txt", true);
            sw.WriteLine($"##");
            sw.WriteLine($"#  ErrDate : {DateTime.Now:dd-MM-yyyy HH:mm:ss}");
            sw.WriteLine($"#  ErrFunc : {fromsub.GetMethod().Name}");
            sw.WriteLine($"#  ErrInfo : {errorMessage}");
            sw.WriteLine($"##");
            sw.Flush();
            sw.Close();
        }

        public void WriteError(Exception errorException, int skipFrame = 2) {
            WriteError(errorException.Message, skipFrame);
        }

    }

}
