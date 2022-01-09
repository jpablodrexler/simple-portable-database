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
    }
}
