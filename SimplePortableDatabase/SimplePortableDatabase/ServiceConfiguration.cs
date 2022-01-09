using Autofac;
using Microsoft.Extensions.DependencyInjection;
using SimplePortableDatabase.Storage;

namespace SimplePortableDatabase
{
    public static class ServiceConfiguration
    {
        public static void AddSimplePortableDatabaseServices(this IServiceCollection services)
        {
            services.AddSingleton<IObjectListStorage, ObjectListStorage>();
            services.AddSingleton<IDataTableStorage, DataTableStorage>();
            services.AddSingleton<IBlobStorage, BlobStorage>();
            services.AddSingleton<IBackupStorage, BackupStorage>();
            services.AddSingleton<IDatabase, Database>();
        }

        public static void RegisterSimplePortableDatabaseTypes(this ContainerBuilder containerBuilder)
        {
            containerBuilder.RegisterType<ObjectListStorage>().As<IObjectListStorage>().SingleInstance();
            containerBuilder.RegisterType<DataTableStorage>().As<IDataTableStorage>().SingleInstance();
            containerBuilder.RegisterType<BlobStorage>().As<IBlobStorage>().SingleInstance();
            containerBuilder.RegisterType<BackupStorage>().As<IBackupStorage>().SingleInstance();
            containerBuilder.RegisterType<Database>().As<IDatabase>().SingleInstance();
        }
    }
}
