/**
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
        MailMessage CreateEmailMessage(string subject, string body, MailAddress from, List<MailAddress> to, List<MailAddress> cc = null, List<MailAddress> bcc = null, List<Attachment> attachments = null);
        Task SendEmailMessage(MailMessage mailMessage);
        Task CreateAndSend(string subject, string body, MailAddress from, List<MailAddress> to, List<MailAddress> cc = null, List<MailAddress> bcc = null, List<Attachment> attachments = null);
    }

    public sealed class CSurel : ISurel {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;
        private readonly IDbHandler _db;

        public CSurel(IApplication app, ILogger logger, IConfig config, IDbHandler db) {
            _app = app;
            _logger = logger;
            _config = config;
            _db = db;
        }

        private async Task<SmtpClient> CreateSmtpClient() {
            int port = int.Parse(await _db.GetMailInfo<string>("MAIL_PORT"));
            return new SmtpClient() {
                Host = await _db.GetMailInfo<string>("MAIL_IP") ?? _config.Get<string>("SmtpServerIpDomain", _app.GetConfig("smtp_server_ip_domain")),
                Port = (port > 0) ? port : _config.Get<int>("SmtpServerPort", int.Parse(_app.GetConfig("smtp_server_port"))),
                Credentials = new NetworkCredential(
                    await _db.GetMailInfo<string>("MAIL_USERNAME") ?? _config.Get<string>("SmtpServerUsername", _app.GetConfig("smtp_server_username"), true),
                    await _db.GetMailInfo<string>("MAIL_PASSWORD") ?? _config.Get<string>("SmtpServerPassword", _app.GetConfig("smtp_server_password"), true)
                )
            };
        }

        public MailAddress CreateEmailAddress(string address, string displayName = null) {
            if (string.IsNullOrEmpty(displayName)) {
                return new MailAddress(address);
            }
            return new MailAddress(address, displayName, Encoding.UTF8);
        }

        public List<MailAddress> CreateEmailAddress(string[] address) {
            List<MailAddress> addresses = new List<MailAddress>();
            foreach(string a in address) {
                addresses.Add(CreateEmailAddress(a));
            }
            return addresses;
        }

        public Attachment CreateEmailAttachment(string filePath) {
            return new Attachment(filePath);
        }

        public List<Attachment> CreateEmailAttachment(string[] filePath) {
            List<Attachment> attachments = new List<Attachment>();
            foreach(string path in filePath) {
                attachments.Add(CreateEmailAttachment(path));
            }
            return attachments;
        }

        public MailMessage CreateEmailMessage(
            string subject,
            string body,
            MailAddress from,
            List<MailAddress> to,
            List<MailAddress> cc = null,
            List<MailAddress> bcc = null,
            List<Attachment> attachments = null
        ) {
            MailMessage mailMessage = new MailMessage() {
                Subject = subject,
                SubjectEncoding = Encoding.UTF8,
                Body = body,
                BodyEncoding = Encoding.UTF8,
                From = from,
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

        public async Task SendEmailMessage(MailMessage mailMessage) {
            SmtpClient smtpClient = await CreateSmtpClient();
            await smtpClient.SendMailAsync(mailMessage);
        }

        public async Task CreateAndSend(
            string subject,
            string body,
            MailAddress from,
            List<MailAddress> to,
            List<MailAddress> cc = null,
            List<MailAddress> bcc = null,
            List<Attachment> attachments = null
        ) {
            Exception e = null;
            try {
                await SendEmailMessage(
                    CreateEmailMessage(
                        subject,
                        body,
                        from,
                        to,
                        cc,
                        bcc,
                        attachments
                    )
                );
            }
            catch (Exception ex) {
                _logger.WriteError(ex);
                e = ex;
            }
            if (e != null) {
                throw e;
            }
        }
    }

}
