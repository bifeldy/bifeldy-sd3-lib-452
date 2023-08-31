/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
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

using ZXing;
using ZXing.Common;
using ZXing.QrCode;
using ZXing.QrCode.Internal;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IQrBar {
        Image Generate128BarCode(string text, int widthPx = 512, int heightPx = 256);
        Image GenerateQrCode(string text, int sizePx = 512, int version = 25);
        Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25);
        Image AddQrCaption(Image qrImage, string caption);
        string ReadTextFromQrBarCode(Image bitmapImage);
    }

    public class CQrBar : IQrBar {

        public CQrBar() {
            //
        }

        public Image Generate128BarCode(string content, int widthPx = 512, int heightPx = 256) {
            BarcodeWriter writer = new BarcodeWriter() {
                Format = BarcodeFormat.CODE_128,
                Options = new EncodingOptions {
                    Width = widthPx,
                    Height = heightPx,
                    PureBarcode = false
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
                    ErrorCorrection = ErrorCorrectionLevel.L
                }
            };
            return writer.Write(content);
        }

        public Image AddQrLogo(Image qrImage, Image overlayImage, double logoScale = 0.25) {
            Bitmap logoImage = new Bitmap(overlayImage, new Size((int)(qrImage.Width * logoScale), (int)(qrImage.Height * logoScale)));
            int deltaHeigth = qrImage.Height - logoImage.Height;
            int deltaWidth = qrImage.Width - logoImage.Width;
            Graphics g = Graphics.FromImage(qrImage);
            g.DrawImage(logoImage, new Point(deltaWidth / 2, deltaHeigth / 2));
            return qrImage;
        }

        public Image AddQrCaption(Image qrImage, string caption) {
            int margin = 20, textHeight = 20;
            Bitmap qrImageExtended = new Bitmap(qrImage.Width, qrImage.Height + textHeight);
            Graphics g = Graphics.FromImage(qrImageExtended);
            Font font = new Font(FontFamily.GenericMonospace, 10);
            SolidBrush frBrush = new SolidBrush(Color.Black);
            SolidBrush bgBrush = new SolidBrush(Color.White);
            StringFormat format = new StringFormat() {
                Alignment = StringAlignment.Center
            };
            g.FillRectangle(bgBrush, 0, 0, qrImageExtended.Width, qrImageExtended.Height);
            g.DrawImage(qrImage, new Point(0, 0));
            RectangleF rect = new RectangleF(margin, qrImageExtended.Height - textHeight - (margin / 2), qrImageExtended.Width - 2 * margin, textHeight);
            g.DrawString(caption, font, frBrush, rect, format);
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
