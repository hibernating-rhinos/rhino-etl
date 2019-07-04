namespace Rhino.Etl.Tests.Fibonacci.Output
{
    using System.Configuration;
    using Rhino.Etl.Core.ConventionOperations;

    public class FibonacciOutput : ConventionOutputCommandOperation
    {
        public FibonacciOutput(string connectionStringName) 
            : base(connectionStringName)
        {
            Command = "INSERT INTO Fibonacci (Id) VALUES(@Id)";
        }

        public FibonacciOutput(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings)
        {
            Command = "INSERT INTO Fibonacci (Id) VALUES(@Id)";
        }
    }
}