namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using Rhino.Etl.Core;

    public class BaseAggregationDslFixture : BaseDslTest
    {
        protected List<Row> rows;

        public BaseAggregationDslFixture(DslTestDatabaseFixture testDatabase) 
            : base(testDatabase)
        {
            rows = new List<Row>();
            AddRow("milk", 15);
            AddRow("milk", 15);
            AddRow("sugar", 10);
            AddRow("sugar", 15);
            AddRow("coffee", 6);
            AddRow("sugar", 3);
        }

        private void AddRow(string name, int price)
        {
            Row row = new Row();
            row["name"] = name;
            row["price"] = price;
            rows.Add(row);
        }
    }
}