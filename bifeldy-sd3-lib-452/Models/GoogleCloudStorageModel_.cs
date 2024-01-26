/**
* 
* Author       :: Basilius Bias Astho Christyono
* Phone        :: (+62) 889 236 6466
* 
* Department   :: IT SD 03
* Mail         :: bias@indomaret.co.id
* 
* Catatan      :: Template Gcs Bucket & Object
*              :: Karena Ambigu Dengan Tipe Data C#
*              :: Model Supaya Tidak Perlu Install Package Nuget Google Drive
* 
*/

using System;
using System.Collections.Generic;
using System.IO;
using Google.Apis.Download;
using Google.Apis.Services;
using Google.Apis.Storage.v1.Data;
using Google.Apis.Upload;

using static Google.Apis.Storage.v1.ObjectsResource;

namespace bifeldy_sd3_lib_452.Models {

    public enum EGcsUploadStatus {
        NotStarted = 0,
        Starting = 1,
        Uploading = 2,
        Completed = 3,
        Failed = 4
    }

    public enum EGcsDownloadStatus {
        NotStarted = 0,
        Downloading = 1,
        Completed = 2,
        Failed = 3
    }

    public class CGcsUploadProgress {
        public EGcsUploadStatus Status { get; set; }
        public long BytesSent { get; set; }
        public Exception Exception { get; set; }
    }

    public class CGcsDownloadProgress {
        public EGcsDownloadStatus Status { get; set; }
        public long BytesDownloaded { get; set; }
        public Exception Exception { get; set; }
    }

    public sealed class GcsBucket : Google.Apis.Storage.v1.Data.Bucket {
        public new WebsiteData Website {
            get => base.Website;
            set => base.Website = value;
        }
        public new VersioningData Versioning {
            get => base.Versioning;
            set => base.Versioning = value;
        }
        public new DateTime? Updated {
            get => base.Updated;
            set => base.Updated = value;
        }
        public new string UpdatedRaw {
            get => base.UpdatedRaw;
            set => base.UpdatedRaw = value;
        }
        public new DateTime? TimeCreated {
            get => base.TimeCreated;
            set => base.TimeCreated = value;
        }
        public new string TimeCreatedRaw {
            get => base.TimeCreatedRaw;
            set => base.TimeCreatedRaw = value;
        }
        public new string StorageClass {
            get => base.StorageClass;
            set => base.StorageClass = value;
        }
        public new string SelfLink {
            get => base.SelfLink;
            set => base.SelfLink = value;
        }
        public new RetentionPolicyData RetentionPolicy {
            get => base.RetentionPolicy;
            set => base.RetentionPolicy = value;
        }
        public new OwnerData Owner {
            get => base.Owner;
            set => base.Owner = value;
        }
        public new string Name {
            get => base.Name;
            set => base.Name = value;
        }
        public new long? Metageneration {
            get => base.Metageneration;
            set => base.Metageneration = value;
        }
        public new LoggingData Logging {
            get => base.Logging;
            set => base.Logging = value;
        }
        public new ulong? ProjectNumber {
            get => base.ProjectNumber;
            set => base.ProjectNumber = value;
        }
        public new string Location {
            get => base.Location;
            set => base.Location = value;
        }
        public new IList<BucketAccessControl> Acl {
            get => base.Acl;
            set => base.Acl = value;
        }
        public new BillingData Billing {
            get => base.Billing;
            set => base.Billing = value;
        }
        public new string LocationType {
            get => base.LocationType;
            set => base.LocationType = value;
        }
        public new bool? DefaultEventBasedHold {
            get => base.DefaultEventBasedHold;
            set => base.DefaultEventBasedHold = value;
        }
        public new IList<ObjectAccessControl> DefaultObjectAcl {
            get => base.DefaultObjectAcl;
            set => base.DefaultObjectAcl = value;
        }
        public new EncryptionData Encryption {
            get => base.Encryption;
            set => base.Encryption = value;
        }
        public new IList<CorsData> Cors {
            get => base.Cors;
            set => base.Cors = value;
        }
        public new IamConfigurationData IamConfiguration {
            get => base.IamConfiguration;
            set => base.IamConfiguration = value;
        }
        public new string Id {
            get => base.Id;
            set => base.Id = value;
        }
        public new string Kind {
            get => base.Kind;
            set => base.Kind = value;
        }
        public new IDictionary<string, string> Labels {
            get => base.Labels;
            set => base.Labels = value;
        }
        public new LifecycleData Lifecycle {
            get => base.Lifecycle;
            set => base.Lifecycle = value;
        }
        public new string ETag {
            get => base.ETag;
            set => base.ETag = value;
        }
    }

    public sealed class GcsObject : Google.Apis.Storage.v1.Data.Object {
        public new OwnerData Owner {
            get => base.Owner;
            set => base.Owner = value;
        }
        public new string RetentionExpirationTimeRaw {
            get => base.RetentionExpirationTimeRaw;
            set => base.RetentionExpirationTimeRaw = value;
        }
        public new DateTime? RetentionExpirationTime {
            get => base.RetentionExpirationTime;
            set => base.RetentionExpirationTime = value;
        }
        public new string SelfLink {
            get => base.SelfLink;
            set => base.SelfLink = value;
        }
        public new ulong? Size {
            get => base.Size;
            set => base.Size = value;
        }
        public new string StorageClass {
            get => base.StorageClass;
            set => base.StorageClass = value;
        }
        public new bool? TemporaryHold {
            get => base.TemporaryHold;
            set => base.TemporaryHold = value;
        }
        public new string TimeCreatedRaw {
            get => base.TimeCreatedRaw;
            set => base.TimeCreatedRaw = value;
        }
        public new DateTime? TimeCreated {
            get => base.TimeCreated;
            set => base.TimeCreated = value;
        }
        public new string TimeDeletedRaw {
            get => base.TimeDeletedRaw;
            set => base.TimeDeletedRaw = value;
        }
        public new DateTime? TimeDeleted {
            get => base.TimeDeleted;
            set => base.TimeDeleted = value;
        }
        public new string TimeStorageClassUpdatedRaw {
            get => base.TimeStorageClassUpdatedRaw;
            set => base.TimeStorageClassUpdatedRaw = value;
        }
        public new DateTime? TimeStorageClassUpdated {
            get => base.TimeStorageClassUpdated;
            set => base.TimeStorageClassUpdated = value;
        }
        public new string UpdatedRaw {
            get => base.UpdatedRaw;
            set => base.UpdatedRaw = value;
        }
        public new DateTime? Updated {
            get => base.Updated;
            set => base.Updated = value;
        }
        public new string Name {
            get => base.Name;
            set => base.Name = value;
        }
        public new long? Metageneration {
            get => base.Metageneration;
            set => base.Metageneration = value;
        }
        public new IDictionary<string, string> Metadata {
            get => base.Metadata;
            set => base.Metadata = value;
        }
        public new string Crc32c {
            get => base.Crc32c;
            set => base.Crc32c = value;
        }
        public new string CacheControl {
            get => base.CacheControl;
            set => base.CacheControl = value;
        }
        public new int? ComponentCount {
            get => base.ComponentCount;
            set => base.ComponentCount = value;
        }
        public new string ContentDisposition {
            get => base.ContentDisposition;
            set => base.ContentDisposition = value;
        }
        public new string ContentEncoding {
            get => base.ContentEncoding;
            set => base.ContentEncoding = value;
        }
        public new string ContentLanguage {
            get => base.ContentLanguage;
            set => base.ContentLanguage = value;
        }
        public new string ContentType {
            get => base.ContentType;
            set => base.ContentType = value;
        }
        public new string MediaLink {
            get => base.MediaLink;
            set => base.MediaLink = value;
        }
        public new CustomerEncryptionData CustomerEncryption {
            get => base.CustomerEncryption;
            set => base.CustomerEncryption = value;
        }
        public new string ETag {
            get => base.ETag;
            set => base.ETag = value;
        }
        public new bool? EventBasedHold {
            get => base.EventBasedHold;
            set => base.EventBasedHold = value;
        }
        public new long? Generation {
            get => base.Generation;
            set => base.Generation = value;
        }
        public new string Id {
            get => base.Id;
            set => base.Id = value;
        }
        public new string Kind {
            get => base.Kind;
            set => base.Kind = value;
        }
        public new string KmsKeyName {
            get => base.KmsKeyName;
            set => base.KmsKeyName = value;
        }
        public new string Md5Hash {
            get => base.Md5Hash;
            set => base.Md5Hash = value;
        }
        public new string Bucket {
            get => base.Bucket;
            set => base.Bucket = value;
        }
        public new IList<ObjectAccessControl> Acl {
            get => base.Acl;
            set => base.Acl = value;
        }
    }

    public sealed class GcsMediaUpload : InsertMediaUpload {

        public GcsMediaUpload(
            IClientService clientService,
            GcsObject body,
            string bucket,
            Stream stream,
            string contentType
        ) : base(clientService, body, bucket, stream, contentType) {
            //
        }

    }

}
