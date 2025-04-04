﻿/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Kirim Email
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Handlers;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ISurel {
        MailAddress CreateEmailAddress(string address, string displayName = null);
        List<MailAddress> CreateEmailAddress(string[] address);
        Attachment CreateEmailAttachment(string filePath);
        List<Attachment> CreateEmailAttachment(string[] filePath);
        MailMessage CreateEmailMessage(string subject, string body, List<MailAddress> to, List<MailAddress> cc = null, List<MailAddress> bcc = null, List<Attachment> attachments = null, MailAddress from = null);
        Task SendEmailMessage(MailMessage mailMessage, bool paksaDariHo = false);
        MailAddress GetDefaultBotSenderFromAddress();
        Task CreateAndSend(string subject, string body, List<MailAddress> to, List<MailAddress> cc = null, List<MailAddress> bcc = null, List<Attachment> attachments = null, MailAddress from = null);
    }

    public sealed class CSurel : ISurel {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;
        private readonly IDbHandler _db;

        public CSurel(IApplication app, ILogger logger, IConfig config, IDbHandler db) {
            this._app = app;
            this._logger = logger;
            this._config = config;
            this._db = db;
        }

        private async Task<SmtpClient> CreateSmtpClient(bool paksaDariHo = false) {
            string host = await this._db.OraPg_GetMailInfo<string>("MAIL_IP");
            string _port = await this._db.OraPg_GetMailInfo<string>("MAIL_PORT");
            int port = string.IsNullOrEmpty(_port) ? 0 : int.Parse(_port);
            string uname = await this._db.OraPg_GetMailInfo<string>("MAIL_USERNAME");
            string upass = await this._db.OraPg_GetMailInfo<string>("MAIL_PASSWORD");

            if (string.IsNullOrEmpty(host) || port <= 0 || string.IsNullOrEmpty(uname) || string.IsNullOrEmpty(upass) || paksaDariHo) {
                host = this._config.Get<string>("SmtpServerIpDomain", this._app.GetConfig("smtp_server_ip_domain"));
                port = this._config.Get<int>("SmtpServerPort", int.Parse(this._app.GetConfig("smtp_server_port")));
                uname = this._config.Get<string>("SmtpServerUsername", this._app.GetConfig("smtp_server_username"), true);
                upass = this._config.Get<string>("SmtpServerPassword", this._app.GetConfig("smtp_server_password"), true);
            }

            return new SmtpClient() {
                Host = host,
                Port = port,
                Credentials = new NetworkCredential(uname, upass)
            };
        }

        public MailAddress CreateEmailAddress(string address, string displayName = null) {
            return string.IsNullOrEmpty(displayName) ? new MailAddress(address) : new MailAddress(address, displayName, Encoding.UTF8);
        }

        public List<MailAddress> CreateEmailAddress(string[] address) {
            var addresses = new List<MailAddress>();
            foreach(string a in address) {
                addresses.Add(this.CreateEmailAddress(a));
            }

            return addresses;
        }

        public Attachment CreateEmailAttachment(string filePath) {
            return new Attachment(filePath);
        }

        public List<Attachment> CreateEmailAttachment(string[] filePath) {
            var attachments = new List<Attachment>();
            foreach(string path in filePath) {
                attachments.Add(this.CreateEmailAttachment(path));
            }

            return attachments;
        }

        public MailAddress GetDefaultBotSenderFromAddress() {
            return this.CreateEmailAddress("sd3@indomaret.co.id", $"[SD3_BOT] 📧 {this._app.AppName} v{this._app.AppVersion}");
        }

        public MailMessage CreateEmailMessage(
            string subject,
            string body,
            List<MailAddress> to,
            List<MailAddress> cc = null,
            List<MailAddress> bcc = null,
            List<Attachment> attachments = null,
            MailAddress from = null
        ) {
            var mailMessage = new MailMessage() {
                Subject = subject,
                SubjectEncoding = Encoding.UTF8,
                Body = body,
                BodyEncoding = Encoding.UTF8,
                From = from ?? this.GetDefaultBotSenderFromAddress(),
                IsBodyHtml = true
            };
            foreach (MailAddress t in to) {
                mailMessage.To.Add(t);
            }

            if (cc != null) {
                foreach (MailAddress c in cc) {
                    mailMessage.CC.Add(c);
                }
            }

            if (bcc != null) {
                foreach (MailAddress b in bcc) {
                    mailMessage.Bcc.Add(b);
                }
            }

            if (attachments != null) {
                foreach (Attachment a in attachments) {
                    mailMessage.Attachments.Add(a);
                }
            }

            return mailMessage;
        }

        public async Task SendEmailMessage(MailMessage mailMessage, bool paksaDariHo = false) {
            SmtpClient smtpClient = await this.CreateSmtpClient(paksaDariHo);
            await smtpClient.SendMailAsync(mailMessage);
        }

        public async Task CreateAndSend(
            string subject,
            string body,
            List<MailAddress> to,
            List<MailAddress> cc = null,
            List<MailAddress> bcc = null,
            List<Attachment> attachments = null,
            MailAddress from = null
        ) {
            Exception e = null;
            try {
                MailMessage mail = this.CreateEmailMessage(
                    subject, body, to, cc, bcc, attachments,
                    from ?? this.GetDefaultBotSenderFromAddress()
                );
                try {
                    // Pakai Regional
                    await this.SendEmailMessage(mail);
                }
                catch {
                    // Via DCHO
                    await this.SendEmailMessage(mail, true);
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex);
                e = ex;
            }

            if (e != null) {
                throw e;
            }
        }
    }

}
