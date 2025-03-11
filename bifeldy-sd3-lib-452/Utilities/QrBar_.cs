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

using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;

using ImageProcessor;
using ImageProcessor.Imaging;

// using QRCoder;
// using ZXing;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IQrBar {
        Image Generate128BarCode(string text, int widthPx = 512, int heightPx = 256);
        Image GenerateQrCodeSquare(string content, int version, int sizePx = 512);
        Image GenerateQrCodeDots(string content, int version = -1, int sizePx = 512);
        Image AddBackground(Image qrImage, Image bgImage);
        Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25);
        Image AddQrCaption(Image qrImage, string caption);
        string ReadTextFromQrBarCode(Image bitmapImage);
    }

    public sealed class CQrBar : IQrBar {

        private readonly int margin = 20;

        public CQrBar() {
            //
        }

        public Image Generate128BarCode(string content, int widthPx = 512, int heightPx = 256) {
            var writer = new ZXing.BarcodeWriter() {
                Format = ZXing.BarcodeFormat.CODE_128,
                Options = new ZXing.Common.EncodingOptions {
                    Width = widthPx,
                    Height = heightPx,
                    PureBarcode = false,
                    NoPadding = true
                }
            };
            return writer.Write(content);
        }

        public Image GenerateQrCodeSquare(string content, int version, int sizePx = 512) {
            var writer = new ZXing.BarcodeWriter() {
                Format = ZXing.BarcodeFormat.QR_CODE,
                Options = new ZXing.QrCode.QrCodeEncodingOptions() {
                    QrVersion = version,
                    Width = sizePx,
                    Height = sizePx,
                    ErrorCorrection = ZXing.QrCode.Internal.ErrorCorrectionLevel.L
                }
            };
            return writer.Write(content);
        }

        public Image GenerateQrCodeDots(string content, int version = -1, int sizePx = 512) {
            var qrGenerator = new QRCoder.QRCodeGenerator();
            QRCoder.QRCodeData qrCodeData = qrGenerator.CreateQrCode(
                content,
                QRCoder.QRCodeGenerator.ECCLevel.L,
                requestedVersion: version
            );
            var qrCode = new QRCoder.ArtQRCode(qrCodeData);
            Bitmap qrImage = qrCode.GetGraphic();
            qrImage.MakeTransparent(Color.White);
            return new Bitmap(qrImage, new Size(sizePx, sizePx));
        }

        public Image AddBackground(Image qrImage, Image bgImage) {
            Image qrBackground = null;
            using (var outStream = new MemoryStream()) {
                using (var imageFactory = new ImageFactory(true)) {
                    _ = imageFactory.Load(bgImage);
                    var size = new Size(qrImage.Width, qrImage.Height);
                    var resizeLayer = new ResizeLayer(size, ResizeMode.Crop, AnchorPosition.TopLeft);
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

        public Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25) {
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
                qrImageExtended = new Bitmap(qrImage.Width, qrImage.Height + font.Height + (2 * this.margin));
                using (var g = Graphics.FromImage(qrImageExtended)) {
                    using (var frBrush = new SolidBrush(Color.Black)) {
                        using (var bgBrush = new SolidBrush(Color.White)) {
                            using (var format = new StringFormat()) {
                                format.Alignment = StringAlignment.Center;
                                g.FillRectangle(bgBrush, 0, 0, qrImageExtended.Width, qrImageExtended.Height);
                                g.DrawImage(qrImage, new Point(0, 0));
                                var rect = new RectangleF(this.margin / 2, qrImageExtended.Height - font.Height - this.margin, qrImageExtended.Width - this.margin, font.Height);
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
                    PossibleFormats = new List<ZXing.BarcodeFormat> {
                        ZXing.BarcodeFormat.QR_CODE,
                        ZXing.BarcodeFormat.CODE_128
                    },
                    TryHarder = true
                }
            };
            ZXing.Result result = reader.Decode((Bitmap) bitmapImage);
            return result.Text;
        }

    }

}
