/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Stream Tools
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IStream {
        string GZipDecompressString(byte[] byteData);
        byte[] GZipCompressString(string text);
        MemoryStream ReadFileAsBinaryByte(string filePath, int maxChunk = 2048);
    }

    public sealed class CStream : IStream {

        public CStream() {
            //
        }

        public string GZipDecompressString(byte[] byteData) {
            byte[] gZipBuffer = byteData;
            using (var memoryStream = new MemoryStream()) {
                int dataLength = BitConverter.ToInt32(gZipBuffer, 0);
                memoryStream.Write(gZipBuffer, 4, gZipBuffer.Length - 4);
                var buffer = new byte[dataLength];
                memoryStream.Position = 0;
                using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Decompress)) {
                    gZipStream.Read(buffer, 0, buffer.Length);
                }
                return Encoding.UTF8.GetString(buffer);
            }
        }

        public byte[] GZipCompressString(string text) {
            byte[] buffer = Encoding.UTF8.GetBytes(text);
            var memoryStream = new MemoryStream();
            using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Compress, true)) {
                gZipStream.Write(buffer, 0, buffer.Length);
            }
            memoryStream.Position = 0;
            var compressedData = new byte[memoryStream.Length];
            memoryStream.Read(compressedData, 0, compressedData.Length);
            var gZipBuffer = new byte[compressedData.Length + 4];
            Buffer.BlockCopy(compressedData, 0, gZipBuffer, 4, compressedData.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(buffer.Length), 0, gZipBuffer, 0, 4);
            return gZipBuffer;
        }

        public MemoryStream ReadFileAsBinaryByte(string filePath, int maxChunk = 2048) {
            MemoryStream dest = new MemoryStream();
            using (Stream source = File.OpenRead(filePath)) {
                byte[] buffer = new byte[maxChunk];
                int bytesRead = 0;
                while ((bytesRead = source.Read(buffer, 0, buffer.Length)) > 0) {
                    dest.Write(buffer, 0, bytesRead);
                }
            }
            return dest;
        }

    }

}
