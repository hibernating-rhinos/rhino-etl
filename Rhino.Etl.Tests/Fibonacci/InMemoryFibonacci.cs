namespace Rhino.Etl.Tests.Fibonacci
{
    using Rhino.Etl.Core;

    public class InMemoryFibonacci : EtlProcess
    {
        public FibonacciOperation FibonacciOperation = new FibonacciOperation(25);

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(FibonacciOperation);
        }
    }
}