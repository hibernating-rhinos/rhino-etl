namespace Rhino.Etl.Tests.LoadTest
{
    using System.Diagnostics;
    using Xunit;
 
    public class LoadTestJoinsFixture
    {
        [Fact]
        public void CanDoLargeJoinsEfficiently()
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            using(Join_250_000_UsersWithMostlyFallingOut proc = new Join_250_000_UsersWithMostlyFallingOut())
            {
                proc.Execute();
                Assert.Equal(15000, proc.operation.count);
            }
            stopwatch.Stop();
            Assert.True(stopwatch.ElapsedMilliseconds < 1000);
        }
    }
}
    