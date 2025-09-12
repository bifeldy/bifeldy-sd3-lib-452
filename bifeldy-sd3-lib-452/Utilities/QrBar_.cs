/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Qr Bar Code
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using ImageProcessor;
using ImageProcessor.Imaging;
using System;
using System.Drawing;
using System.IO;
using System.Windows.Forms;

// using QRCoder;
// using ZXing;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IQrBar {
        Image Generate128BarCode(string text, int minWidthPx = 512, int heightPx = 192, bool withPadding = true);
        Image GenerateQrCode(string content, int version = -1, int minSizePx = 512);
        Image AddBackground(Image qrImage, Image bgImage);
        Image AddLogo(Image qrImage, Image overlayImage, double logoScale);
        Image AddQrCaption(Image qrImage, string caption);
        string ReadTextFromQrBarCode(Image bitmapImage);
    }

    public sealed class CQrBar : IQrBar {

        private static readonly int MAX_RETRY = 5;
        private static readonly int MARGIN = 20;

        public CQrBar() {
            //
        }

        public Image Generate128BarCode(string content, int minWidthPx = 512, int heightPx = 192, bool withPadding = true) {
            var writer = new ZXing.BarcodeWriter() {
                Format = ZXing.BarcodeFormat.CODE_128,
                Options = new ZXing.Common.EncodingOptions {
                    PureBarcode = false,
                    NoPadding = true,
                    Margin = 0
                }
            };

            using (Bitmap generatedImage = writer.Write(content)) {
                generatedImage.MakeTransparent(Color.White);

                int retry = 0;
                int sizeLimit = minWidthPx;
                int padding = withPadding ? 10 : 0;

                var drawLocation = new Point(padding, padding);

                while (retry <= MAX_RETRY) {
                    var res = new Bitmap(sizeLimit, heightPx);

                    var resizeLayer = new ResizeLayer(
                        new Size(
                            sizeLimit - (padding * 2),
                            heightPx - (padding * 2)
                        ),
                        ResizeMode.Stretch
                    );

                    using (var img = new Bitmap(generatedImage)) {
                        using (var imageFactory = new ImageFactory(true)) {
                            _ = imageFactory.Load(img);
                            _ = imageFactory.Resize(resizeLayer);

                            using (var outStream = new MemoryStream()) {
                                _ = imageFactory.Save(outStream);

                                using (var imgResized = Image.FromStream(outStream)) {
                                    using (var g = Graphics.FromImage(res)) {
                                        g.DrawImage(imgResized, drawLocation);

                                        string qrText = this.ReadTextFromQrBarCode(res);
                                        if (content == qrText) {
                                            return res;
                                        }

                                        retry++;
                                        sizeLimit = minWidthPx + (minWidthPx * retry / 2);
                                    }

                                }
                            }
                        }
                    }

                    res.Dispose();
                }

                throw new Exception("Hasil Bar Code Tidak Terbaca, Mohon Perbesar Resolusi");
            }
        }

        public Image GenerateQrCode(string content, int version = -1, int minSizePx = 512) {
            var qrGenerator = new QRCoder.QRCodeGenerator();
            QRCoder.QRCodeData qrCodeData = qrGenerator.CreateQrCode(
                content,
                QRCoder.QRCodeGenerator.ECCLevel.L,
                requestedVersion: version
            );
            var qrCode = new QRCoder.ArtQRCode(qrCodeData);

            using (Bitmap generatedImage = qrCode.GetGraphic()) {
                generatedImage.MakeTransparent(Color.White);

                int retry = 0;
                int sizeLimit = minSizePx;

                var drawLocation = new Point(0, 0);

                while (retry <= MAX_RETRY) {
                    var res = new Bitmap(sizeLimit, sizeLimit);

                    var resizeLayer = new ResizeLayer(
                        new Size(sizeLimit, sizeLimit),
                        ResizeMode.Stretch
                    );

                    using (var img = new Bitmap(generatedImage)) {
                        using (var imageFactory = new ImageFactory(true)) {
                            _ = imageFactory.Load(img);
                            _ = imageFactory.Resize(resizeLayer);

                            using (var outStream = new MemoryStream()) {
                                _ = imageFactory.Save(outStream);

                                using (var imgResized = Image.FromStream(outStream)) {
                                    using (var g = Graphics.FromImage(res)) {
                                        g.DrawImage(imgResized, drawLocation);

                                        string qrText = this.ReadTextFromQrBarCode(res);
                                        if (content == qrText) {
                                            return res;
                                        }

                                        retry++;
                                        sizeLimit = minSizePx + (minSizePx * retry / 2);
                                    }

                                }
                            }
                        }
                    }

                    res.Dispose();
                }

                throw new Exception("Hasil QR Code Tidak Terbaca, Mohon Perbesar Resolusi");
            }
        }

        public Image AddBackground(Image qrImage, Image bgImage) {
            Image qrBackground = null;
            using (var outStream = new MemoryStream()) {
                using (var imageFactory = new ImageFactory(true)) {
                    _ = imageFactory.Load(bgImage);
                    var size = new Size(qrImage.Width, qrImage.Height);
                    var resizeLayer = new ResizeLayer(size, ResizeMode.Crop);
                    _ = imageFactory.Resize(resizeLayer).Brightness(25).Alpha(75).Save(outStream);
                    qrBackground = Image.FromStream(outStream);
                    ((Bitmap) qrImage).MakeTransparent(Color.White);

                    using (var g = Graphics.FromImage(qrBackground)) {
                        g.DrawImage(qrImage, new Point(0, 0));
                    }
                }
            }

            return qrBackground;
        }

        public Image AddLogo(Image qrImage, Image overlayImage, double logoScale) {
            logoScale = Math.Min(Math.Max(logoScale, 0.15), 0.25);

            var logoImage = new Bitmap(overlayImage, new Size((int)(qrImage.Width * logoScale), (int)(qrImage.Height * logoScale)));
            int deltaHeigth = qrImage.Height - logoImage.Height;
            int deltaWidth = qrImage.Width - logoImage.Width;

            using (var g = Graphics.FromImage(qrImage)) {
                g.DrawImage(logoImage, new Point(deltaWidth / 2, deltaHeigth / 2));
            }

            return qrImage;
        }

        public Image AddQrCaption(Image qrImage, string caption) {
            var qrImageExtended = (Bitmap) qrImage;

            using (var font = new Font(FontFamily.GenericMonospace, (float) qrImage.Width / Math.Max(caption.Length, 45))) {
                qrImageExtended = new Bitmap(qrImage.Width, qrImage.Height + font.Height + (2 * MARGIN));

                using (var g = Graphics.FromImage(qrImageExtended)) {
                    using (var frBrush = new SolidBrush(Color.Black)) {
                        using (var bgBrush = new SolidBrush(Color.White)) {
                            using (var format = new StringFormat()) {
                                format.Alignment = StringAlignment.Center;
                                g.FillRectangle(bgBrush, 0, 0, qrImageExtended.Width, qrImageExtended.Height);
                                g.DrawImage(qrImage, new Point(0, 0));
                                var rect = new RectangleF(MARGIN / 2, qrImageExtended.Height - font.Height - MARGIN, qrImageExtended.Width - MARGIN, font.Height);
                                g.DrawString(caption, font, frBrush, rect, format);
                            }
                        }
                    }
                }
            }

            return qrImageExtended;
        }

        public string ReadTextFromQrBarCode(Image bitmapImage) {
            ZXing.IBarcodeReader reader = new ZXing.BarcodeReader() {
                AutoRotate = true,
                Options = new ZXing.Common.DecodingOptions {
                    PureBarcode = false,
                    TryInverted = true,
                    TryHarder = true
                }
            };
            ZXing.Result result = reader.Decode((Bitmap) bitmapImage);
            return result.Text;
        }

    }

}
