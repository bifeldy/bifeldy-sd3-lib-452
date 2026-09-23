/**
* 
* Author       :: Basilius Bias Astho Christyono
* Phone        :: (+62) 889 236 6466
* 
* Department   :: IT SD 03
* Mail         :: bias@indomaret.co.id
* 
* Catatan      :: Template S3 Bucket & Object
*              :: Karena Ambigu Dengan Tipe Data C#
*              :: Model Supaya Tidak Perlu Install Package Nuget AWSSDK
* 
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace bifeldy_sd3_lib_452.Models {

    public sealed class AwsS3Prefix {
        public string BucketName { get; set; }
        public string Prefix { get; set; }
    }

    public sealed class AwsS3Bucket {
        public string BucketArn { get; set; }
        public DateTime CreationDate { get; set; }
        public string BucketName { get; set; }
        public string BucketRegion { get; set; }
    }

    public sealed class AwsS3Object {
        public List<string> ChecksumAlgorithm { get; set; }
        public string ETag { get; set; }
        public string BucketName { get; set; }
        public string Key { get; set; }
        public DateTime LastModified { get; set; }

        // Dipecah dari class Owner milik AWS agar aman dari CS0012
        public string OwnerId { get; set; }
        public string OwnerDisplayName { get; set; }

        // Diubah ke string dari class bawaan AWS
        public bool IsRestoreInProgress { get; set; }
        public long Size { get; set; }
        public string StorageClass { get; set; }
        public string ChecksumType { get; set; }
    }

    // Menggunakan IDisposable agar ResponseStream dan koneksi AWS bisa di-close
    public sealed class AwsS3GetObjectResponse : IDisposable {

        public Stream ResponseStream { get; set; }
        public long ContentLength { get; set; }
        public HttpStatusCode HttpStatusCode { get; set; }

        public string AcceptRanges { get; set; }
        public bool BucketKeyEnabled { get; set; }
        public string BucketName { get; set; }
        public string ChecksumCRC32 { get; set; }
        public string ChecksumCRC32C { get; set; }
        public string ChecksumCRC64NVME { get; set; }
        public string ChecksumMD5 { get; set; }
        public string ChecksumSHA1 { get; set; }
        public string ChecksumSHA256 { get; set; }
        public string ChecksumSHA512 { get; set; }
        public string ChecksumType { get; set; }
        public string ChecksumXXHASH128 { get; set; }
        public string ChecksumXXHASH3 { get; set; }
        public string ChecksumXXHASH64 { get; set; }
        public string ContentRange { get; set; }
        public string DeleteMarker { get; set; }
        public string ETag { get; set; }

        // Expiration AWS dipecah
        public string ExpirationRuleId { get; set; }
        public DateTime? ExpirationDateUtc { get; set; }

        public string ExpiresString { get; set; }
        public string Key { get; set; }
        public DateTime LastModified { get; set; }
        public int MissingMeta { get; set; }
        public string ObjectLockLegalHoldStatus { get; set; }
        public string ObjectLockMode { get; set; }
        public DateTime ObjectLockRetainUntilDate { get; set; }
        public int? PartsCount { get; set; }
        public string ReplicationStatus { get; set; }
        public string RequestCharged { get; set; }
        public DateTime? RestoreExpiration { get; set; }
        public bool RestoreInProgress { get; set; }
        public string ServerSideEncryptionCustomerMethod { get; set; }
        public string ServerSideEncryptionKeyManagementServiceKeyId { get; set; }
        public string ServerSideEncryptionMethod { get; set; }
        public string StorageClass { get; set; }
        public int TagCount { get; set; }
        public string VersionId { get; set; }
        public string WebsiteRedirectLocation { get; set; }

        // Diubah menjadi Dictionary bawaan C# agar aman
        public Dictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Metadata { get; set; } = new Dictionary<string, string>();

        // Menyimpan objek asli AWS untuk keperluan pembersihan memori (Dispose)
        private readonly IDisposable _awsOriginalResponse;

        public AwsS3GetObjectResponse(IDisposable awsOriginalResponse) {
            this._awsOriginalResponse = awsOriginalResponse;
        }

        public void Dispose() {
            this.ResponseStream?.Dispose();
            this._awsOriginalResponse?.Dispose();
        }
    }
}