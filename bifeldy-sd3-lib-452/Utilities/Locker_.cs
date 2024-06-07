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
        SemaphoreSlim MutexGlobalApp { get; }
        Semaphore SemaphoreGlobalSys(string name, int initialCount = 1, int maximumCount = 1);
        SemaphoreSlim SemaphoreGlobalApp(string name, int initialCount = 1, int maximumCount = 1);
    }

    public sealed class CLocker : ILocker {

        private readonly IApplication _app;
        private readonly IDictionary<string, Semaphore> semaphore_global_sys = new Dictionary<string, Semaphore>();
        private readonly IDictionary<string, SemaphoreSlim> semaphore_global_app = new Dictionary<string, SemaphoreSlim>();

        public CLocker(IApplication app) {
            this._app = app;
            //
            this.MutexGlobalSys = new Semaphore(1, 1, this._app.AppName);
            this.MutexGlobalApp = new SemaphoreSlim(1, 1);
        }

        public Semaphore MutexGlobalSys { get; } = null;

        public SemaphoreSlim MutexGlobalApp { get; } = null;

        public Semaphore SemaphoreGlobalSys(string name, int initialCount = 1, int maximumCount = 1) {
            if (!this.semaphore_global_sys.ContainsKey(name)) {
                var semaphoreSecurity = new SemaphoreSecurity(name, AccessControlSections.All);
                var semaphore = new Semaphore(initialCount, maximumCount, name, out bool createdNew, semaphoreSecurity);
                if (!createdNew) {
                    semaphore = Semaphore.OpenExisting(name, SemaphoreRights.Synchronize);
                }

                this.semaphore_global_sys.Add(name, semaphore);
            }

            return this.semaphore_global_sys[name];
        }

        public SemaphoreSlim SemaphoreGlobalApp(string name, int initialCount = 1, int maximumCount = 1) {
            if (!this.semaphore_global_app.ContainsKey(name)) {
                this.semaphore_global_app.Add(name, new SemaphoreSlim(initialCount, maximumCount));
            }

            return this.semaphore_global_app[name];
        }

    }

}
