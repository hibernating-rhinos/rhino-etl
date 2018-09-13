namespace Rhino.Etl.Tests.Integration
{
    using System.Collections.Generic;
    using System.Data;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;
    
    public class DatabaseToDatabaseWithTransformations : BaseUserToPeopleTest
    {
        [Fact]
        public void CanCopyTableWithTransform()
        {
            using(UsersToPeople process = new UsersToPeople())
                process.Execute();
            
            System.Collections.Generic.List<string[]> names = Use.Transaction<List<string[]>>("test", delegate(IDbCommand cmd)
            {
                System.Collections.Generic.List<string[]> tuples = new System.Collections.Generic.List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }

        [Fact]
        public void CanCopyTableWithTransformFromConnectionStringSettings()
        {
            using (UsersToPeopleFromConnectionStringSettings process = new UsersToPeopleFromConnectionStringSettings())
                process.Execute();

            System.Collections.Generic.List<string[]> names = Use.Transaction<List<string[]>>("test", delegate(IDbCommand cmd)
            {
                System.Collections.Generic.List<string[]> tuples = new System.Collections.Generic.List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }       
    }
}