using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Enumerables;
using Rhino.Etl.Core.Operations;
using Rhino.Etl.Core.Pipelines;
using Rhino.Etl.Tests.Joins;
using Rhino.Mocks;
using Xunit;
using Xunit.Sdk;

namespace Rhino.Etl.Tests
{
    public class MultiJoinOperationTest : TestBase
    {
        [Fact]
        public void can_multi_join_on_different_column_combinations()
        {
            var userRecords = TestOperation(
                new SampleMultiJoinOperation()
                    .Left(new InputUsers2Operation())
                    .Right(new InputPersons2Operation())
                );
            AreSameRows(new Row[] {
                        Row.FromObject(new FullUserInfo2 { Id = 1, Name = "Jim1", Address = "1215 Smith St." }),
                        Row.FromObject(new FullUserInfo2 { Id = 2, Name = "Jim2", Address = "1216 Smith St." }),
                        Row.FromObject(new FullUserInfo2 { Id = 3, Name = "Jim3", Address = "1217 Smith St." })
                        }
            , userRecords);
        }



    }


    public class SampleMultiJoinOperation : MultiJoinOperation
    {

        protected override void SetupJoinConditions()
        {
            MultiJoin(InnerJoin.Left("Id").Right("Id"))
            .Or(LeftJoin.Left("Name").Right("Name"));
            //.And(LeftJoin.Left("Name").Right("Name"));
        }


        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            var row = leftRow.Clone();
            row["Address"] = rightRow["Address"];

            return row;
        }
    }


    class InputUsers2Operation : AbstractOperation
    {

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            yield return Row.FromObject(new User2 { Id = 1, Name = "Jim1" });
            yield return Row.FromObject(new User2 { Id = 2, Name = "Jim2" });
            yield return Row.FromObject(new User2 { Id = 3, Name = "Jim3" });
        }

    }
    class InputPersons2Operation : AbstractOperation
    {

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            yield return Row.FromObject(new AddressInfo2 { Id = 1, Address = "1215 Smith St." });
            yield return Row.FromObject(new AddressInfo2 { Id = 2, Address = "1216 Smith St." });
            yield return Row.FromObject(new AddressInfo2 { Id = -1, Name = "Jim3", Address = "1217 Smith St." });
        }

    }


    public class User2
    {
        public int Id;
        public string Name;
    }
    public class AddressInfo2
    {
        public int Id;
        public string Name;
        public string Address;
    }
    public class FullUserInfo2
    {
        public int Id;
        public string Name;
        public string Address;
    }
}
