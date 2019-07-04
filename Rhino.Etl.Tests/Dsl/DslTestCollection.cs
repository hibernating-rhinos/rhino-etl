namespace Rhino.Etl.Tests.Dsl
{
    using Xunit;

    [CollectionDefinition(Name, DisableParallelization = true)]
    public class DslTestCollection : ICollectionFixture<DslTestDatabaseFixture>
    {
        public const string Name = "DslTest";
    }
}