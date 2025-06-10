/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: External API Call
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IApi {
        HttpClient CreateHttpClient(int timeoutSeconds = 600);
        Task<HttpResponseMessage> HeadData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> GetData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, HttpCompletionOption readOpt = HttpCompletionOption.ResponseContentRead, Encoding encoding = null);
        Task<HttpResponseMessage> DeleteData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> PostData(string urlPath, dynamic objBody, bool multipart = false, List<Tuple<string, string>> headerOpts = null, string[] contentKeyName = null, string[] contentType = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> PutData(string urlPath, dynamic objBody, bool multipart = false, List<Tuple<string, string>> headerOpts = null, string[] contentKeyName = null, string[] contentType = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> ConnectData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> OptionsData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> PatchData(string urlPath, dynamic objBody, bool multipart = false, List<Tuple<string, string>> headerOpts = null, string[] contentKeyName = null, string[] contentType = null, int timeoutSeconds = 600, Encoding encoding = null);
        Task<HttpResponseMessage> TraceData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null);
    }

    public sealed class CApi : IApi {

        private readonly IConverter _converter;

        public CApi(IConverter converter) {
            this._converter = converter;
        }

        private async Task<HttpContent> GetHttpContent(dynamic httpContent, string contentType, Encoding encoding = null) {
            HttpContent content = null;

            if (encoding == null) {
                encoding = Encoding.UTF8;
            }

            if (httpContent.GetType() == typeof(string)) {
                content = new StringContent(httpContent, encoding, contentType);
            }
            else if (typeof(HttpRequest).IsAssignableFrom(httpContent.GetType())) {
                using (var ms = new MemoryStream()) {
                    await httpContent.Body.CopyToAsync(ms);
                    await ms.FlushAsync();
                    byte[] arr = ms.ToArray();
                    content = new ByteArrayContent(arr);
                }
            }
            else if (typeof(Stream).IsAssignableFrom(httpContent.GetType())) {
                content = new StreamContent(httpContent);
            }
            else if (httpContent.GetType() == typeof(byte[])) {
                content = new ByteArrayContent(httpContent);
            }
            else {
                content = new StringContent(this._converter.ObjectToJson(httpContent), encoding, contentType);
            }

            content.Headers.ContentType = MediaTypeHeaderValue.Parse(contentType);
            return content;
        }

        private async Task<HttpRequestMessage> FetchApi(
            string httpUri, HttpMethod httpMethod,
            dynamic httpContent = null, bool multipart = false, List<Tuple<string, string>> httpHeaders = null,
            string[] contentKeyName = null, string[] contentType = null,
            Encoding encoding = null
        ) {
            var httpRequestMessage = new HttpRequestMessage() {
                Method = httpMethod,
                RequestUri = new Uri(httpUri)
            };

            if (httpContent != null) {
                if (multipart) {
                    // Send binary form data with key value
                    // file=...binary...;
                    var lsContent = new List<HttpContent>();

                    if (httpContent.GetType().IsArray) {
                        for (int i = 0; i < httpContent.Length; i++) {
                            lsContent.Add(
                                await GetHttpContent(
                                    httpContent[i],
                                    contentType?.Length > 0 ? contentType[i] : "application/octet-stream",
                                    encoding ?? Encoding.UTF8
                                )
                            );
                        }
                    }
                    else {
                        lsContent.Add(await GetHttpContent(httpContent, "application/octet-stream"));
                    }

                    httpContent = new MultipartFormDataContent();
                    for (int i = 0; i < lsContent.Count; i++) {
                        httpContent.Add(lsContent[i], contentKeyName?.Length > 0 ? contentKeyName[i] : "file");
                    }
                }
                else {
                    httpContent = await GetHttpContent(
                        httpContent,
                        contentType?.Length > 0 ? contentType[0] : "application/json"
                    );
                }

                httpRequestMessage.Content = httpContent;
            }

            if (httpHeaders != null) {
                foreach (Tuple<string, string> hdr in httpHeaders) {
                    try {
                        httpRequestMessage.Headers.Add(hdr.Item1, hdr.Item2);
                    }
                    catch {
                        // Skip Invalid Header ~
                    }
                }
            }

            return httpRequestMessage;
        }

        public HttpClient CreateHttpClient(int timeoutSeconds = 600) {
            return new HttpClient() {
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            };
        }

        public async Task<HttpResponseMessage> HeadData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await this.FetchApi(urlPath, HttpMethod.Head, httpHeaders: headerOpts, encoding: encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> GetData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, HttpCompletionOption readOpt = HttpCompletionOption.ResponseContentRead, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await this.FetchApi(urlPath, HttpMethod.Get, httpHeaders: headerOpts, encoding: encoding ?? Encoding.UTF8), readOpt);
        }

        public async Task<HttpResponseMessage> DeleteData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await this.FetchApi(urlPath, HttpMethod.Delete, httpHeaders: headerOpts, encoding: encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> PostData(string urlPath, dynamic objBody, bool multipart = false, List<Tuple<string, string>> headerOpts = null, string[] contentKeyName = null, string[] contentType = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await FetchApi(urlPath, HttpMethod.Post, objBody, multipart, headerOpts, contentKeyName, contentType, encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> PutData(string urlPath, dynamic objBody, bool multipart = false, List<Tuple<string, string>> headerOpts = null, string[] contentKeyName = null, string[] contentType = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await FetchApi(urlPath, HttpMethod.Put, objBody, multipart, headerOpts, contentKeyName, contentType, encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> ConnectData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await this.FetchApi(urlPath, new HttpMethod("CONNECT"), httpHeaders: headerOpts, encoding: encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> OptionsData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await this.FetchApi(urlPath, new HttpMethod("OPTIONS"), httpHeaders: headerOpts, encoding: encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> PatchData(string urlPath, dynamic objBody, bool multipart = false, List<Tuple<string, string>> headerOpts = null, string[] contentKeyName = null, string[] contentType = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await FetchApi(urlPath, new HttpMethod("PATCH"), objBody, multipart, headerOpts, contentKeyName, contentType, encoding ?? Encoding.UTF8));
        }

        public async Task<HttpResponseMessage> TraceData(string urlPath, List<Tuple<string, string>> headerOpts = null, int timeoutSeconds = 600, Encoding encoding = null) {
            return await this.CreateHttpClient(timeoutSeconds).SendAsync(await this.FetchApi(urlPath, HttpMethod.Trace, httpHeaders: headerOpts, encoding: encoding ?? Encoding.UTF8));
        }

    }

}
