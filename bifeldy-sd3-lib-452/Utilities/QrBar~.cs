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

using ZXing;
using ZXing.Common;
using ZXing.QrCode;
using ZXing.QrCode.Internal;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IQrBar {
        Image Generate128BarCode(string text, int widthPx = 512, int heightPx = 256);
        Image GenerateQrCode(string text, int sizePx = 512, int version = 25);
        Image AddBackground(Image qrImage, Image bgImage);
        Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25);
        Image AddQrCaption(Image qrImage, string caption);
        string ReadTextFromQrBarCode(Image bitmapImage);
    }

    public class CQrBar : IQrBar {

        private readonly IConverter _converter;

        public CQrBar(IConverter converter) {
            _converter = converter;
        }

        public Image Generate128BarCode(string content, int widthPx = 512, int heightPx = 256) {
            BarcodeWriter writer = new BarcodeWriter() {
                Format = BarcodeFormat.CODE_128,
                Options = new EncodingOptions {
                    Width = widthPx,
                    Height = heightPx,
                    PureBarcode = false,
                    NoPadding = true
                }
            };
            return writer.Write(content);
        }

        public Image GenerateQrCode(string content, int sizePx = 512, int version = 25) {
            BarcodeWriter writer = new BarcodeWriter() {
                Format = BarcodeFormat.QR_CODE,
                Options = new QrCodeEncodingOptions() {
                    QrVersion = version,
                    Width = sizePx,
                    Height = sizePx,
                    ErrorCorrection = ErrorCorrectionLevel.L,
                    NoPadding = true,
                    Margin = 10
                }
            };
            return writer.Write(content);
        }

        public Image AddBackground(Image qrImage, Image bgImage) {
            Image qrBackground = null;
            using (MemoryStream outStream = new MemoryStream()) {
                using (ImageFactory imageFactory = new ImageFactory(true)) {
                    imageFactory.Load(bgImage);
                    Size size = new Size(qrImage.Width, qrImage.Height);
                    ResizeLayer resizeLayer = new ResizeLayer(size, ResizeMode.Crop, AnchorPosition.TopLeft);
                    imageFactory.Resize(resizeLayer).Alpha(50).Save(outStream);
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
            IBarcodeReader reader = new BarcodeReader() {
                AutoRotate = true,
                Options = new DecodingOptions {
                    PossibleFormats = new List<BarcodeFormat> {
                        BarcodeFormat.QR_CODE,
                        BarcodeFormat.CODE_128
                    },
                    TryHarder = true
                }
            };
            Result result = reader.Decode((Bitmap) bitmapImage);
            return result.Text;
        }

    }

}
