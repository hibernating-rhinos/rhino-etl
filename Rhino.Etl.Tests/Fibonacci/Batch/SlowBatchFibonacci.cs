#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Errors;
    using Rhino.Etl.Tests.Fibonacci.Output;

    public class SlowBatchFibonacci : EtlProcess
    {
        private readonly string _connectionStringName;
        private readonly int _max;
        private readonly Should _should;

        public SlowBatchFibonacci(string connectionStringName, int max, Should should)
        {
            _connectionStringName = connectionStringName;
            _max = max;
            _should = should;
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new FibonacciOperation(_max));
            if (_should == Should.Throw)
                Register(new ThrowingOperation());
            Register(new SlowBatchFibonacciToDatabase(_connectionStringName));
        }
    }
}
#endif