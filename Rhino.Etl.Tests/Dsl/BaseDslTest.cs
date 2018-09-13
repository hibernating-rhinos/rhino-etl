namespace Rhino.Etl.Tests.Dsl
{
    using System;
    using System.IO;
    using System.Reflection;
    using Rhino.Etl.Core;
    using Rhino.Etl.Dsl;
    using Xunit;

    [CollectionDefinition(Name, DisableParallelization = true)]
    public class DslTestCollection : ICollectionFixture<TestDatabaseFixture>
    {
        public const string Name = "DslTest";
    }

    [Collection(DslTestCollection.Name)]
    public class BaseDslTest
    {
        protected static EtlProcess CreateDslInstance(string url)
        {
            var baseDir = Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath);
            return EtlDslEngine.Factory.Create<EtlProcess>(Path.Combine(baseDir, url));
        }
    }
}