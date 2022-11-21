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
        string LogInfo { get; }
        string LogErrorFolderPath { get; }
        void ClearLog();
        void WriteLog(string subject, string body, bool newLine = false);
        void WriteError(string errorMessage, int skipFrame = 1);
        void WriteError(Exception errorException, int skipFrame = 2);
    }

    public sealed class CLogger : ILogger, INotifyPropertyChanged {

        public event PropertyChangedEventHandler PropertyChanged;

        private readonly IApp _app;

        public string LogErrorFolderPath { get; }

        private string LogInformation = "";

        public CLogger(IApp app) {
            _app = app;

            LogErrorFolderPath = Path.Combine(_app.AppLocation, "Error_Logs");
            if (!Directory.Exists(LogErrorFolderPath)) {
                Directory.CreateDirectory(LogErrorFolderPath);
            }
        }

        public string LogInfo {
            get => LogInformation;
            set {
                LogInformation = value;
                NotifyPropertyChanged("LogInfo");
            }
        }

        public void NotifyPropertyChanged(string propertyName) {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        public void WriteLog(string subject, string body, bool newLine = false) {
            string content = $"[{DateTime.Now}] {subject} :: {body} {Environment.NewLine}";
            if (newLine) {
                content += Environment.NewLine;
            }
            LogInfo = (content + LogInformation);
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

        public void ClearLog() {
            LogInfo = "";
        }

    }

}
