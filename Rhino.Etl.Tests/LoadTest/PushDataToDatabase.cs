namespace Rhino.Etl.Tests.LoadTest
{
    using Rhino.Etl.Core;

    public class PushDataToDatabase : EtlProcess
    {
        private readonly string _connectionStringName;
        private readonly int _expectedCount;

        public PushDataToDatabase(string connectionStringName, int expectedCount)
        {
            _connectionStringName = connectionStringName;
            _expectedCount = expectedCount;
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new GenerateUsers(_expectedCount));
            Register(new BulkInsertUsers(_connectionStringName));
        }
    }
}