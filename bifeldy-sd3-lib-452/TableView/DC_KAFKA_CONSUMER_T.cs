/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Tabel DC_KAFKA_CONSUMER_T
 *              :: Seharusnya Tidak Untuk Didaftarkan Ke DI Container
 * 
 */

using System;

namespace bifeldy_sd3_lib_452.TableView {

    public sealed class DC_KAFKA_CONSUMER_T {
        public string TPC { set; get; }
        public decimal OFFS { set; get; }
        public string KEY { set; get; }
        public string VAL { set; get; }
        public DateTime TMSTAMP { set; get; }
    }

}
