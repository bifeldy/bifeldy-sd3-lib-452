/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Thread Safe Inter-Locking
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System.Collections.Generic;
using System.Security.AccessControl;
using System.Threading;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ILocker {
        Semaphore MutexGlobalSys { get; }
        SemaphoreSlim MutexLocalApp { get; }
        Semaphore SemaphoreGlobalSys(string name, int initialCount = 0, int maximumCount = 0);
        SemaphoreSlim SemaphoreLocalApp(string name, int initialCount = 0, int maximumCount = 0);
    }

    public sealed class CLocker : ILocker {

        private readonly IApplication _app;

        private Semaphore mutex_global_sys = null;
        private readonly IDictionary<string, Semaphore> semaphore_global_sys = new Dictionary<string, Semaphore>();

        private SemaphoreSlim mutex_local_app = null;
        private readonly IDictionary<string, SemaphoreSlim> semaphore_local_app = new Dictionary<string, SemaphoreSlim>();

        public CLocker(IApplication app) {
            _app = app;
            //
            mutex_global_sys = new Semaphore(1, 1, _app.AppName);
            mutex_local_app = new SemaphoreSlim(1, 1);
        }

        public Semaphore MutexGlobalSys => mutex_global_sys;

        public SemaphoreSlim MutexLocalApp => mutex_local_app;

        public Semaphore SemaphoreGlobalSys(string name, int initialCount = 0, int maximumCount = 0) {
            if (!semaphore_global_sys.ContainsKey(name)) {
                SemaphoreSecurity semaphoreSecurity = new SemaphoreSecurity(name, AccessControlSections.All);
                Semaphore semaphore = new Semaphore(initialCount, maximumCount, name, out bool createdNew, semaphoreSecurity);
                if (!createdNew) {
                    semaphore = Semaphore.OpenExisting(name, SemaphoreRights.Synchronize);
                }
                semaphore_global_sys.Add(name, semaphore);
            }
            return semaphore_global_sys[name];
        }

        public SemaphoreSlim SemaphoreLocalApp(string name, int initialCount = 0, int maximumCount = 0) {
            if (!semaphore_local_app.ContainsKey(name)) {
                semaphore_local_app.Add(name, new SemaphoreSlim(initialCount, maximumCount));
            }
            return semaphore_local_app[name];
        }

    }

}
