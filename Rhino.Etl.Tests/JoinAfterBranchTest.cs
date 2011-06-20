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
    public class JoinAfterBranchTest : TestBase
    {
        [Fact]
        public void can_join_with_current_pipline_input()
        {
            var userRecords = TestOperation(
                        new InputUsersOperation(),
                        new RightJoinWith(new InputPersonsOperation())
                );
            AreSameRows(new Row[] {
                        Row.FromObject(new FullUserInfo { Id = 1, Name = "Jim1", Address = "1215 Smith St." }),
                        Row.FromObject(new FullUserInfo { Id = 2, Name = "Jim2", Address = "1216 Smith St." })
                        }
            , userRecords);
        }

        [Fact]
        public void can_join_with_after_branch()
        {
            var userRecords = new List<Row>();
            var dummy = TestOperation(
                        new InputUsersOperation(),
                        new BranchingOperation()
                            .Add(
                                 new PartialProcessOperation()
                                     .Register(    new RightJoinWith(new InputPersonsOperation()))
                                     .RegisterLast(new ResultsOperation(userRecords)))
                );
            AreSameRows(new Row[] {
                        Row.FromObject(new FullUserInfo { Id = 1, Name = "Jim1", Address = "1215 Smith St." }),
                        Row.FromObject(new FullUserInfo { Id = 2, Name = "Jim2", Address = "1216 Smith St." })
                        }
            , userRecords);
        }

    }


    public class RightJoinWith : RightJoinWithOperation
    {
        public RightJoinWith(IOperation rightOp)
            : base(rightOp)
        {
        }
        protected override void SetupJoinConditions()
        {
            InnerJoin.Left("Id").Right("Id");
        }
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            var row = leftRow.Clone();
            row["Address"] = rightRow["Address"];

            return row;
        }
    }

    class InputUsersOperation : AbstractOperation
    {

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            yield return Row.FromObject(new User { Id = 1, Name = "Jim1" });
            yield return Row.FromObject(new User { Id = 2, Name = "Jim2" });
        }

    }
    class InputPersonsOperation : AbstractOperation
    {

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            yield return Row.FromObject(new AddressInfo { Id = 1, Address = "1215 Smith St." });
            yield return Row.FromObject(new AddressInfo { Id = 2, Address = "1216 Smith St." });
        }

    }


    public class User
    {
        public int Id;
        public string Name;
    }
    public class AddressInfo
    {
        public int Id;
        public string Address;
    }
    public class FullUserInfo
    {
        public int Id;
        public string Name;
        public string Address;
    }
}
