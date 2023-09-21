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
        Image GenerateQrCodeDots(string content, int version = -1);
        Image AddBackground(Image qrImage, Image bgImage);
        Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25);
        Image AddQrCaption(Image qrImage, string caption);
        string ReadTextFromQrBarCode(Image bitmapImage);
    }

    public sealed class CQrBar : IQrBar {

        public CQrBar() {
            //
        }

        public Image Generate128BarCode(string content, int widthPx = 512, int heightPx = 256) {
            ZXing.BarcodeWriter writer = new ZXing.BarcodeWriter() {
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
            ZXing.BarcodeWriter writer = new ZXing.BarcodeWriter() {
                Format = ZXing.BarcodeFormat.QR_CODE,
                Options = new ZXing.QrCode.QrCodeEncodingOptions() {
                    QrVersion = version,
                    Width = sizePx,
                    Height = sizePx,
                    ErrorCorrection = ZXing.QrCode.Internal.ErrorCorrectionLevel.L,
                    NoPadding = true,
                    Margin = 10
                }
            };
            return writer.Write(content);
        }

        public Image GenerateQrCodeDots(string content, int version = -1) {
            QRCoder.QRCodeGenerator qrGenerator = new QRCoder.QRCodeGenerator();
            QRCoder.QRCodeData qrCodeData = qrGenerator.CreateQrCode(
                content,
                QRCoder.QRCodeGenerator.ECCLevel.L,
                requestedVersion: version
            );
            QRCoder.ArtQRCode qrCode = new QRCoder.ArtQRCode(qrCodeData);
            return qrCode.GetGraphic();
        }

        public Image AddBackground(Image qrImage, Image bgImage) {
            Image qrBackground = null;
            using (MemoryStream outStream = new MemoryStream()) {
                using (ImageFactory imageFactory = new ImageFactory(true)) {
                    imageFactory.Load(bgImage);
                    Size size = new Size(qrImage.Width, qrImage.Height);
                    ResizeLayer resizeLayer = new ResizeLayer(size, ResizeMode.Crop, AnchorPosition.TopLeft);
                    imageFactory.Resize(resizeLayer).Brightness(25).Alpha(75).Save(outStream);
                    qrBackground = Image.FromStream(outStream);
                    ((Bitmap) qrImage).MakeTransparent(Color.White);
                    using (Graphics g = Graphics.FromImage(qrBackground)) {
                        g.DrawImage(qrImage, new Point(0, 0));
                    }
                }
            }
            return qrBackground;
        }

        public Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25) {
            Bitmap logoImage = new Bitmap(overlayImage, new Size((int)(qrImage.Width * logoScale), (int)(qrImage.Height * logoScale)));
            int deltaHeigth = qrImage.Height - logoImage.Height;
            int deltaWidth = qrImage.Width - logoImage.Width;
            using (Graphics g = Graphics.FromImage(qrImage)) {
                g.DrawImage(logoImage, new Point(deltaWidth / 2, deltaHeigth / 2));
            }
            return qrImage;
        }

        public Image AddQrCaption(Image qrImage, string caption) {
            int margin = 20, textHeight = 20;
            Bitmap qrImageExtended = new Bitmap(qrImage.Width, qrImage.Height + margin + textHeight);
            using (Graphics g = Graphics.FromImage(qrImageExtended)) {
                using (Font font = new Font(FontFamily.GenericMonospace, 10)) {
                    using (SolidBrush frBrush = new SolidBrush(Color.Black)) {
                        using (SolidBrush bgBrush = new SolidBrush(Color.White)) {
                            using (StringFormat format = new StringFormat()) {
                                format.Alignment = StringAlignment.Center;
                                g.FillRectangle(bgBrush, 0, 0, qrImageExtended.Width, qrImageExtended.Height);
                                g.DrawImage(qrImage, new Point(0, 0));
                                RectangleF rect = new RectangleF(margin / 2, qrImageExtended.Height - textHeight - (margin / 2), qrImageExtended.Width - margin, textHeight);
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
