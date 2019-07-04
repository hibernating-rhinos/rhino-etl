namespace Rhino.Etl.Tests.Dsl
{
    using System;
    using System.IO;
    using System.Reflection;
    using Rhino.Etl.Core;
    using Rhino.Etl.Dsl;
    using Xunit;

    [Collection(DslTestCollection.Name)]
    public class BaseDslTest
    {
        public BaseDslTest(DslTestDatabaseFixture testDatabase)
        {
            this.TestDatabase = testDatabase;
        }

        public TestDatabaseFixture TestDatabase { get; }

        protected static EtlProcess CreateDslInstance(string url)
        {
            var baseDir = Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath);
            return EtlDslEngine.Factory.Create<EtlProcess>(Path.Combine(baseDir, url));
        }
    }
}