#if FEATURE_SQLCOMMANDSET
namespace Rhino.Etl.Tests.Fibonacci.Batch
{
    using Rhino.Etl.Tests.Errors;
    using Rhino.Etl.Tests.Fibonacci.Output;

    public class BatchFibonacciFromConnectionStringSettings : BatchFibonacci
    {
        public BatchFibonacciFromConnectionStringSettings(int max, Should should)
            : base(max, should)
        {
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new FibonacciOperation(this.Max));
            if (this.Should == Should.Throw)
                Register(new ThrowingOperation());
            Register(new BatchFibonacciToDatabaseFromConnectionStringSettings());
        }
    }
}
#endif