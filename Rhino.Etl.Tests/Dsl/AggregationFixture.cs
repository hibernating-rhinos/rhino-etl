namespace Rhino.Etl.Tests.Dsl
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Aggregation;
    using Rhino.Etl.Tests.Joins;
    using Xunit;

    [Collection("Dsl")]
    public class AggregationFixture : BaseAggregationFixture
    {
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
            Assert.Equal(1, operation.Results.Count);
            Assert.Equal("[milk, sugar, coffee]", operation.Results[0]["result"]);
        }
    }
}