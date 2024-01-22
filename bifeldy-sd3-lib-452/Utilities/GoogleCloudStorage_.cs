/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Google Cloud Storage
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Web;

using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Services;
using Google.Apis.Storage.v1;
using Google.Apis.Upload;

using Google.Cloud.Storage.V1;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IGoogleCloudStorage {
        void LoadCredential(string pathFile = null);
        void InitializeClient();
        Task<List<GcsBucket>> ListAllBuckets();
        Task<List<GcsObject>> ListAllObjects(string path, string prefix = "", string delimiter = "");
        Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload);
        Task<IGcsUploadProgress> UploadFile(FileInfo fileInfo, string targetFolderId, Stream stream, Uri uploadSession = null, Action<IGcsUploadProgress> uploadProgress = null);
        Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<IGcsDownloadProgress> downloadProgress = null);
        Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow);
    }

    public sealed class CGoogleCloudStorage : IGoogleCloudStorage {

        private readonly ILogger _logger;
        private readonly IConverter _converter;

        private string credentialPath = string.Empty;
        private string projectId = string.Empty;

        private GoogleCredential googleCredential = null;
        private StorageService storageService = null;
        private UrlSigner urlSigner = null;

        public CGoogleCloudStorage(ILogger logger, IConverter converter) {
            _logger = logger;
            _converter = converter;
        }

        public void LoadCredential(string pathFile) {
            credentialPath = pathFile;
            if (string.IsNullOrEmpty(credentialPath) || !File.Exists(credentialPath)) {
                throw new Exception("Lokasi file credential.json tidak valid");
            }
            string text = File.ReadAllText(credentialPath);
            IDictionary<string, string> json = _converter.JsonToObject<Dictionary<string, string>>(text);
            json.TryGetValue("project_id", out projectId);
            _logger.WriteInfo($"{GetType().Name}Credential", text);
            googleCredential = GoogleCredential.FromFile(credentialPath).CreateScoped(StorageService.Scope.DevstorageFullControl);
            urlSigner = UrlSigner.FromServiceAccountPath(credentialPath);
        }

        public void InitializeClient() {
            if (googleCredential == null) {
                LoadCredential(credentialPath);
            }
            storageService = new StorageService(
                new BaseClientService.Initializer() {
                    HttpClientInitializer = googleCredential,
                    ApplicationName = projectId
                }
            );
            _logger.WriteInfo($"{GetType().Name}Client", storageService.Name);
        }

        public async Task<List<GcsBucket>> ListAllBuckets() {
            if (storageService == null) {
                InitializeClient();
            }
            List<GcsBucket> allBuckets = new List<GcsBucket>();

            BucketsResource.ListRequest request = storageService.Buckets.List(storageService.ApplicationName);
            request.Fields = "nextPageToken, items";

            ulong pageNum = 1;
            string pageToken = null;
            do {
                _logger.WriteInfo($"{GetType().Name}LoadBucketPage", $"{pageNum}");

                request.PageToken = pageToken;
                Google.Apis.Storage.v1.Data.Buckets buckets = await request.ExecuteAsync();
                pageToken = buckets.NextPageToken;
                if (buckets != null) {
                    if (buckets.Items != null) {
                        foreach (Google.Apis.Storage.v1.Data.Bucket bucket in buckets.Items) {
                            string bucketJson = _converter.ObjectToJson(bucket);
                            GcsBucket bucketObj = _converter.JsonToObject<GcsBucket>(bucketJson);
                            allBuckets.Add(bucketObj);
                        }
                    }
                }

                pageNum++;
            }
            while (pageToken != null);

            return allBuckets;
        }

        public async Task<List<GcsObject>> ListAllObjects(string path, string prefix = "", string delimiter = "") {
            if (storageService == null) {
                InitializeClient();
            }
            List<GcsObject> allObjects = new List<GcsObject>();

            ObjectsResource.ListRequest request = storageService.Objects.List(path);
            request.Fields = "nextPageToken, items";
            if (!string.IsNullOrEmpty(prefix)) {
                request.Prefix = prefix;
            }
            if (!string.IsNullOrEmpty(delimiter)) {
                request.Delimiter = delimiter;
            }

            ulong pageNum = 1;
            string pageToken = null;
            do {
                _logger.WriteInfo($"{GetType().Name}LoadObjectPage", $"{pageNum}");

                request.PageToken = pageToken;
                Google.Apis.Storage.v1.Data.Objects objects = await request.ExecuteAsync();
                pageToken = objects.NextPageToken;
                if (objects != null) {
                    if (objects.Items != null) {
                        foreach (Google.Apis.Storage.v1.Data.Object obj in objects.Items) {
                            string objJson = _converter.ObjectToJson(obj);
                            GcsObject objObj = _converter.JsonToObject<GcsObject>(objJson);
                            allObjects.Add(objObj);
                        }
                    }
                }

                pageNum++;
            }
            while (pageToken != null);

            return allObjects;
        }

        public async Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload) {
            return await mediaUpload.InitiateSessionAsync();
        }

        public async Task<IGcsUploadProgress> UploadFile(FileInfo fileInfo, string bucketName, Stream stream, Uri uploadSession = null, Action<IGcsUploadProgress> uploadProgress = null) {
            GcsObject obj = new GcsObject {
                Name = fileInfo.Name,
                Bucket = bucketName,
                ContentType = MimeMapping.GetMimeMapping(fileInfo.Name),
            };

            GcsMediaUpload mediaUpload = (GcsMediaUpload) storageService.Objects.Insert(obj, obj.Bucket, stream, obj.ContentType);
            mediaUpload.Fields = "id, name, size, contentType";
            mediaUpload.ChunkSize = ResumableUpload.MinimumChunkSize;

            if (uploadSession == null) {
                uploadSession = await CreateUploadUri(mediaUpload);
            }
            if (uploadProgress != null) {
                mediaUpload.ProgressChanged += (Action<IUploadProgress>) uploadProgress;
            }

            _logger.WriteInfo($"{GetType().Name}UploadStart", $"{fileInfo.Name} ===>>> {bucketName} :: {fileInfo.Length} Bytes");
            IUploadProgress result = await mediaUpload.ResumeAsync(uploadSession);
            _logger.WriteInfo($"{GetType().Name}UploadCompleted", $"{fileInfo.Name} ===>>> {bucketName} :: 100 %");
            return (IGcsUploadProgress) result;
        }

        public async Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<IGcsDownloadProgress> downloadProgress = null) {
            ObjectsResource.GetRequest request = storageService.Objects.Get(fileObj.Bucket, fileObj.Name);

            request.MediaDownloader.ChunkSize = ResumableUpload.MinimumChunkSize;

            if (downloadProgress != null) {
                request.MediaDownloader.ProgressChanged += (Action<IDownloadProgress>) downloadProgress;
            }

            _logger.WriteInfo($"{GetType().Name}DownloadStart", $"{fileObj.Bucket}/{fileObj.Name} <<<=== {fileLocalPath} :: {fileObj.Size} Bytes");

            using (FileStream fs = new FileStream(fileLocalPath, FileMode.Create, FileAccess.Write)) {
                await request.DownloadAsync(fs);
                _logger.WriteInfo($"{GetType().Name}DownloadCompleted", $"{fileObj.Bucket}/{fileObj.Name} <<<=== {fileLocalPath} :: 100 %");
            }
        }

        public async Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow) {
            string ddl = await urlSigner.SignAsync(fileObj.Bucket, fileObj.Name, expiredDurationFromNow);
            _logger.WriteInfo($"{GetType().Name}DirectDownloadLink", ddl);
            return ddl;
        }

    }

}
