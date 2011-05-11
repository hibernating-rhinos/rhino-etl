using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using Xunit;
using Xunit.Sdk;

namespace Rhino.Etl.Tests
{
    public abstract class TestBase
    {
        protected void AreSameRows(IList<Row> expected, IList<Row> actual)
        {
            if (expected.Count != actual.Count)
                throw new AssertException("Expected count of " + expected.Count + " not same as actual: " + actual.Count);

            for (int i = 0; i < expected.Count; i++)
                Assert.True(expected[i].Equals(actual[i]), expected[i].ToString() + " not equal to " + actual[i].ToString());
        }

        protected void AreSameRowsAsExpected(IList<Row> expected, IList<Row> actual)
        {
            if (expected.Count != actual.Count)
                throw new AssertException("Expected count of " + expected.Count + " not same as actual: " + actual.Count);

            var fields = new List<string>();
            for (int k = 0; k < expected[0].Columns.Count(); k++)
            {
                var fieldName = expected[0].Columns.ElementAt(k);
                if (expected[0][fieldName] != null)
                    fields.Add(expected[0].Columns.ElementAt(k));
            }
            for (int i = 0; i < expected.Count; i++)
            {

                foreach (var fieldName in fields)
                    Assert.True(
                        (object.ReferenceEquals(expected[i][fieldName], null) &&
                        object.ReferenceEquals(actual[i][fieldName], null)) ||
                        expected[i][fieldName].Equals(actual[i][fieldName]),
                        "Line #" + i + " " + fieldName + ": '" + Convert.ToString(expected[i][fieldName]) + "' not equal to '" + Convert.ToString(actual[i][fieldName]) + "'");
            }
        }
        protected List<Row> TestOperation(params IOperation[] operations)
        {
            return new TestProcess(operations).ExecuteWithResults();
        }
        //protected List<Row> TestProcess(EtlProcess process)
        //{
        //    return process.ExecuteWithResults();
        //}

        protected class TestProcess : EtlProcess
        {
            List<Row> returnRows = new List<Row>();


            public TestProcess(params IOperation[] testOperations)
            {
                this.testOperations = testOperations;
            }

            IEnumerable<IOperation> testOperations = null;

            protected override void Initialize()
            {
                foreach (var testOperation in testOperations)
                    Register(testOperation);

                Register(new ResultsOperation(returnRows));
            }

            public List<Row> ExecuteWithResults()
            {
                Execute();
                return returnRows;
            }

            protected override void PostProcessing()
            {
                base.PostProcessing();

                //enumerate errors and throw first one
                //event fired after op completes never fires on error 
                //so this is only chance I know for generic exception throwing, 
                //otherwise they get ignored
                var list = GetAllErrors().ToList();
                foreach (var error in list)
                    throw error;

            }
        }
        
        protected class ResultsOperation : AbstractOperation
        {
            public ResultsOperation(List<Row> returnRows)
            {
                this.returnRows = returnRows;
            }

            List<Row> returnRows = null;

            public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
            {
                returnRows.AddRange(rows);

                return rows;
            }
        }

    }
}
