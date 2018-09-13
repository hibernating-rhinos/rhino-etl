#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Errors;
    using Rhino.Etl.Tests.Fibonacci.Output;

    public class BatchFibonacci : EtlProcess
    {
        private readonly int max;

        protected int Max
        {
            get
            {
                return max;
            }
        }

        private readonly Should should;
        protected Should Should
        {
            get
            {
                return should;
            }
        }

        public BatchFibonacci(int max, Should should)
        {
            this.max = max;
            this.should = should;
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new FibonacciOperation(max));
            if (should == Should.Throw)
                Register(new ThrowingOperation());
            Register(new BatchFibonacciToDatabase());
        }
    }
}
#endif