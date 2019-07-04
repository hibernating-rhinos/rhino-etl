namespace Rhino.Etl.Tests.Fibonacci.Output
{
    using System.Configuration;
    using Rhino.Etl.Core;
    using Rhino.Etl.Tests.Errors;

    public class OutputFibonacciToDatabase : EtlProcess
    {
        private readonly int max;
        private readonly Should should;
        public ThrowingOperation ThrowingOperation { get; } = new ThrowingOperation();
        public FibonacciOutput OutputOperation { get; }

        public OutputFibonacciToDatabase(string connectionStringName, int max, Should should)
        {
            this.max = max;
            this.should = should;
            this.OutputOperation = new FibonacciOutput(connectionStringName);
        }

        public OutputFibonacciToDatabase(ConnectionStringSettings connectionString, int max, Should should)
        {
            this.max = max;
            this.should = should;
            this.OutputOperation = new FibonacciOutput(connectionString);
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new FibonacciOperation(max));
            if (should == Should.Throw)
                Register(ThrowingOperation);
            Register(OutputOperation);
        }
    }
}
