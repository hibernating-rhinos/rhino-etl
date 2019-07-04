namespace Rhino.Etl.Tests.LoadTest
{
    using Core;

    public class UpperCaseUserNames : EtlProcess
    {
        private readonly string _connectionStringName;

        public UpperCaseUserNames(string connectionStringName)
        {
            _connectionStringName = connectionStringName;
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected override void Initialize()
        {
            Register(new ReadUsers(_connectionStringName));
            Register(new UpperCaseColumn("Name"));
        }
    }
}