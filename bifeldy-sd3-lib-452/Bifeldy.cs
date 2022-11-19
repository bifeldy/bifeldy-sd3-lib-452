/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Bifeldy's Initial Main Application
 * 
 */

using Autofac;
using Autofac.Builder;

namespace bifeldy_sd3_lib_452 {

    public class Bifeldy {

        private IContainer _container = null;
        private ContainerBuilder _builder = null;

        public Bifeldy() {
            _builder = new ContainerBuilder();

            /* Other Solution Project */
            // builder.RegisterAssemblyTypes(Assembly.Load(nameof(nama_project_lain_yang_mau_di_import)))
            //     .Where(vsSln => vsSln.Namespace.Contains("nama_namespace_yang_mau_di_import"))
            //     .As(c => c.GetInterfaces().FirstOrDefault(i => i.Name == "I" + c.Name);
        }

        /// <summary>Di Panggil Sebelum Start();</summary>
        /// <typeparam name="CClass">Nama Class Yang Ingin Di Daftarkan</typeparam>
        /// <typeparam name="IInterface">Nama Interface Dari Class Yang Ingin Di Daftarkan</typeparam>
        /// <param name="singleton">Menggunakan Instance Yang Sama Untuk Keseluruhan Program</param>
        public void RegisterDI<CClass, IInterface>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = _builder.RegisterType<CClass>().As<IInterface>();
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public CClass StartAppplication<CClass>() {
            if (_container == null) {
                _container = _builder.Build();
            }
            return _container.Resolve<CClass>();
        }

    }

}
