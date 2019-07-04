namespace Rhino.Etl.Tests.Dsl
{
    using System.IO;
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Joins;
    using Xunit;

    public class WireEtlProcessEventsFixture : BaseAggregationDslFixture
    {
        public WireEtlProcessEventsFixture(DslTestDatabaseFixture testDatabase) 
            : base(testDatabase)
        { }

        [Fact]
        public void CanCompileWithRowProcessedEvent()
        {
            using (EtlProcess process = CreateDslInstance("Dsl/WireRowProcessedEvent.boo"))
                Assert.NotNull(process);    
        }

        [Fact]
        public void CheckIfOnRowProcessedEventWasWired()
        {
            using (var process = CreateDslInstance("Dsl/WireRowProcessedEvent.boo"))
            {
                process.Register(new GenericEnumerableOperation(rows));
                ResultsToList operation = new ResultsToList();
                process.RegisterLast(operation);
                process.Execute();
                Assert.Single(operation.Results);
                Assert.Equal("[chocolate, sugar, coffee]", operation.Results[0]["result"]);
            }
        }

        [Fact]
        public void CanCompileWithFinishedProcessingEvent()
        {
            using (var process = CreateDslInstance("Dsl/WireOnFinishedProcessingEvent.boo"))
            {
                Assert.NotNull(process);
            }
        }

        [Fact]
        public void CheckIfOnFinishedProcessingEventWasWired()
        {
            using (var process = CreateDslInstance("Dsl/WireOnFinishedProcessingEvent.boo"))
            {
                process.Register(new GenericEnumerableOperation(rows));
                ResultsToList operation = new ResultsToList();
                process.RegisterLast(operation);
                process.Execute();
                Assert.Single(operation.Results);
                Assert.True(File.Exists(@"OnFinishedProcessing.wired"));

                File.Delete(@"OnFinishedProcessing.wired");
            }

        }
    }
}
