/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Tidak Untuk Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace bifeldy_sd3_lib_452.Libraries {
    
    public static class DefaultIcons {

        private static readonly Lazy<Icon> _lazyFolderIcon = new Lazy<Icon>(FetchIcon, true);

        public static Icon FolderLarge => _lazyFolderIcon.Value;

        private static Icon FetchIcon() {
            string tmpDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString())).FullName;
            Icon icon = ExtractFromPath(tmpDir);
            Directory.Delete(tmpDir);
            return icon;
        }

        private static Icon ExtractFromPath(string path) {
            var shinfo = new SHFILEINFO();
            SHGetFileInfo(
                path,
                0, ref shinfo, (uint) Marshal.SizeOf(shinfo),
                SHGFI_ICON | SHGFI_LARGEICON
            );
            return System.Drawing.Icon.FromHandle(shinfo.hIcon);
        }

        [DllImport("Shell32.dll", EntryPoint = "ExtractIconExW", CharSet = CharSet.Unicode, ExactSpelling = true, CallingConvention = CallingConvention.StdCall)]
        private static extern int ExtractIconEx(string sFile, int iIndex, out IntPtr piLargeVersion, out IntPtr piSmallVersion, int amountIcons);

        // https://renenyffenegger.ch/development/Windows/PowerShell/examples/WinAPI/ExtractIconEx/shell32.html
        public static Icon Extract(string file, int number, bool largeIcon) {
            ExtractIconEx(file, number, out IntPtr large, out IntPtr small, 1);
            try {
                return Icon.FromHandle(largeIcon ? large : small);
            }
            catch {
                return null;
            }
        }

        // Struct used by SHGetFileInfo function
        [StructLayout(LayoutKind.Sequential)]
        private struct SHFILEINFO {
            public IntPtr hIcon;
            public int iIcon;
            public uint dwAttributes;

            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)]
            public string szDisplayName;

            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 80)]
            public string szTypeName;
        };

        [DllImport("shell32.dll")]
        private static extern IntPtr SHGetFileInfo(string pszPath, uint dwFileAttributes, ref SHFILEINFO psfi, uint cbSizeFileInfo, uint uFlags);

        private const uint SHGFI_ICON = 0x100;
        private const uint SHGFI_LARGEICON = 0x0;
        private const uint SHGFI_SMALLICON = 0x000000001;

        public static Icon ConvertToIco(Image img, int size) {
            Icon icon;
            using (var msImg = new MemoryStream()) {
                using (var msIco = new MemoryStream()) {
                    img.Save(msImg, ImageFormat.Png);
                    using (var bw = new BinaryWriter(msIco)) {
                        bw.Write((short) 0);           // 0-1 reserved
                        bw.Write((short) 1);           // 2-3 image type, 1 = icon, 2 = cursor
                        bw.Write((short) 1);           // 4-5 number of images
                        bw.Write((byte) size);         // 6 image width
                        bw.Write((byte) size);         // 7 image height
                        bw.Write((byte) 0);            // 8 number of colors
                        bw.Write((byte) 0);            // 9 reserved
                        bw.Write((short) 0);           // 10-11 color planes
                        bw.Write((short) 32);          // 12-13 bits per pixel
                        bw.Write((int) msImg.Length);  // 14-17 size of image data
                        bw.Write(22);                  // 18-21 offset of image data
                        bw.Write(msImg.ToArray());     // write image data
                        bw.Flush();
                        bw.Seek(0, SeekOrigin.Begin);
                        icon = new Icon(msIco);
                    }
                }
            }

            return icon;
        }

        public static async Task<Icon> GetIconFromUrl(string url) {
            var httpClient = new HttpClient();
            using (Stream stream = await httpClient.GetStreamAsync(url)) {
                using (var ms = new MemoryStream()) {
                    stream.CopyTo(ms);
                    ms.Seek(0, SeekOrigin.Begin); // See https://stackoverflow.com/a/72205381/640195
                    var img = Image.FromStream(ms);
                    return ConvertToIco(img, 64);
                }
            }
        }

    }

}
