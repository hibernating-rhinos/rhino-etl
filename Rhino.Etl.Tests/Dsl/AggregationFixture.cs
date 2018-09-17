namespace Rhino.Etl.Tests.Dsl
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Joins;
    using Xunit;

    public class AggregationFixture : BaseAggregationDslFixture
    {
        public AggregationFixture(DslTestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanCompile()
        {
            EtlProcess process = CreateDslInstance("Dsl/Aggregate.boo");
            Assert.NotNull(process);
        }

        [Fact]
        public void CanPerformAggregationFromDsl()
        {
            EtlProcess process = CreateDslInstance("Dsl/Aggregate.boo");
            process.Register(new GenericEnumerableOperation(rows));
            ResultsToList operation = new ResultsToList();
            process.RegisterLast(operation);
            process.Execute();
            Assert.Single(operation.Results);
            Assert.Equal("[milk, sugar, coffee]", operation.Results[0]["result"]);
        }
    }
}