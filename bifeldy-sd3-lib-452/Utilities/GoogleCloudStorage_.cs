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
using Google.Apis.Storage.v1;
using Google.Apis.Upload;

using Google.Cloud.Storage.V1;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IGoogleCloudStorage {
        void LoadCredential(string pathFile = null);
        Task InitializeClient();
        Task<List<GcsBucket>> ListAllBuckets();
        Task<List<GcsObject>> ListAllObjects(string path, string prefix = "", string delimiter = "");
        Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload);
        Task<IGcsUploadProgress> UploadFile(FileInfo fileInfo, string targetFolderId, Stream stream, Uri uploadSession = null, Action<IGcsUploadProgress> uploadProgress = null);
        Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<IGcsDownloadProgress> downloadProgress = null);
    }

    public sealed class CGoogleCloudStorage : IGoogleCloudStorage {

        private readonly ILogger _logger;

        public string CredentialPath { get; set; } = string.Empty;

        private GoogleCredential googleCredential = null;
        private StorageClient storageClient = null;
        private UrlSigner urlSigner = null;

        public CGoogleCloudStorage(ILogger logger) {
            _logger = logger;
        }

        public void LoadCredential(string pathFile) {
            CredentialPath = pathFile;
            if (string.IsNullOrEmpty(CredentialPath) || !File.Exists(CredentialPath)) {
                throw new Exception("Lokasi file credential.json tidak valid");
            }
            _logger.WriteInfo($"{GetType().Name}Credential", File.ReadAllText(CredentialPath));
            googleCredential = GoogleCredential.FromFile(CredentialPath).CreateScoped(StorageService.Scope.DevstorageFullControl);
            urlSigner = UrlSigner.FromServiceAccountPath(CredentialPath);
        }

        public async Task InitializeClient() {
            if (googleCredential == null) {
                LoadCredential(CredentialPath);
            }
            storageClient = await StorageClient.CreateAsync(googleCredential);
            _logger.WriteInfo($"{GetType().Name}Client", storageClient.Service.Name);
        }

        public async Task<List<GcsBucket>> ListAllBuckets() {
            if (storageClient == null) {
                await InitializeClient();
            }
            List<GcsBucket> allBuckets = new List<GcsBucket>();

            BucketsResource.ListRequest request = storageClient.Service.Buckets.List(storageClient.Service.ApplicationName);
            request.Fields = "nextPageToken, items";

            ulong pageNum = 1;
            string pageToken = null;
            do {
                _logger.WriteInfo($"{GetType().Name}LoadBucketPage", $"{pageNum}");

                request.PageToken = pageToken;
                GcsBuckets buckets = (GcsBuckets)await request.ExecuteAsync();
                pageToken = buckets.NextPageToken;
                if (buckets != null) {
                    if (buckets.Items != null) {
                        foreach (GcsBucket bucket in buckets.Items) {
                            allBuckets.Add(bucket);
                        }
                    }
                }

                pageNum++;
            }
            while (pageToken != null);

            return allBuckets;
        }

        public async Task<List<GcsObject>> ListAllObjects(string path, string prefix = "", string delimiter = "") {
            if (storageClient == null) {
                await InitializeClient();
            }
            List<GcsObject> allObjects = new List<GcsObject>();

            ObjectsResource.ListRequest request = storageClient.Service.Objects.List(path);
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
                GcsObjects objects = (GcsObjects)await request.ExecuteAsync();
                pageToken = objects.NextPageToken;
                if (objects != null) {
                    if (objects.Items != null) {
                        foreach (GcsObject obj in objects.Items) {
                            allObjects.Add(obj);
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

            GcsMediaUpload mediaUpload = (GcsMediaUpload)storageClient.Service.Objects.Insert(obj, obj.Bucket, stream, obj.ContentType);
            mediaUpload.Fields = "id, name, size, contentType";
            mediaUpload.ChunkSize = ResumableUpload.MinimumChunkSize;

            if (uploadSession == null) {
                uploadSession = await CreateUploadUri(mediaUpload);
            }
            if (uploadProgress != null) {
                mediaUpload.ProgressChanged += (Action<IUploadProgress>)uploadProgress;
            }

            _logger.WriteInfo($"{GetType().Name}UploadStart", $"{fileInfo.Name} ===>>> {bucketName} :: {fileInfo.Length} Bytes");
            IUploadProgress result = await mediaUpload.ResumeAsync(uploadSession);
            _logger.WriteInfo($"{GetType().Name}UploadCompleted", $"{fileInfo.Name} ===>>> {bucketName} :: 100 %");
            return (IGcsUploadProgress)result;
        }

        public async Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<IGcsDownloadProgress> downloadProgress = null) {
            ObjectsResource.GetRequest request = storageClient.Service.Objects.Get(fileObj.Bucket, fileObj.Name);

            request.MediaDownloader.ChunkSize = ResumableUpload.MinimumChunkSize;

            if (downloadProgress != null) {
                request.MediaDownloader.ProgressChanged += (Action<IDownloadProgress>)downloadProgress;
            }

            _logger.WriteInfo($"{GetType().Name}DownloadStart", $"{fileObj.Bucket}/{fileObj.Name} <<<=== {fileLocalPath} :: {fileObj.Size} Bytes");

            using (FileStream fs = new FileStream(fileLocalPath, FileMode.Create, FileAccess.Write)) {
                await request.DownloadAsync(fs);
                _logger.WriteInfo($"{GetType().Name}DownloadCompleted", $"{fileObj.Bucket}/{fileObj.Name} <<<=== {fileLocalPath} :: 100 %");
            }
        }

        public async Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow) {
            return await urlSigner.SignAsync(fileObj.Bucket, fileObj.Name, expiredDurationFromNow);
        }

    }

}
