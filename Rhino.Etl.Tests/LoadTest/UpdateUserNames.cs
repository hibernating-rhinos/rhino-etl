namespace Rhino.Etl.Tests.LoadTest
{
    using Rhino.Etl.Core.ConventionOperations;

    public class UpdateUserNames : ConventionOutputCommandOperation
    {
        public UpdateUserNames(string connectionStringName)
            : base(connectionStringName)
        {
            Command = "UPDATE Users SET Name = @Name, TestMsg = 'UpperCased' WHERE Id = @Id";
        }
    }
}