/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Branch Connection Induk & Cabangnya
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Reflection;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IBranchCabang {
        List<DC_TABEL_V> GetListBranchConnection(string kodeDcInduk);
    }

    public sealed class CBranchCabang : IBranchCabang {

        private readonly IApi _api;

        public CBranchCabang(IApi api) {
            _api = api;
        }

        public List<DC_TABEL_V> GetListBranchConnection(string kodeDcInduk) {
            throw new Exception("Fitur Belum Di Implementasikan");
        }

    }

}
