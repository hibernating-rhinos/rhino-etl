namespace Rhino.Etl.Tests.Aggregation
{
    using System.Collections.Generic;
    using Rhino.Etl.Core;
    using Xunit;
    
    public class AggregationFixture
    {
        private readonly List<Row> _rows;

        public AggregationFixture()
        {
            _rows = new List<Row>();
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
            _rows.Add(row);
        }

        [Fact]
        public void AggregateRowCount()
        {
            using (RowCount rowCount = new RowCount())
            {
                IEnumerable<Row> result = rowCount.Execute(_rows);
                List<Row> items = new List<Row>(result);
                Assert.Single(items);
                Assert.Equal(6, items[0]["count"]);
            }
        }

        [Fact]
        public void AggregateCostPerProduct()
        {
            using (CostPerProductAggregation aggregation = new CostPerProductAggregation())
            {
                IEnumerable<Row> result = aggregation.Execute(_rows);
                List<Row> items = new List<Row>(result);
                Assert.Equal(3, items.Count);
                Assert.Equal("milk", items[0]["name"]);
                Assert.Equal("sugar", items[1]["name"]);
                Assert.Equal("coffee", items[2]["name"]);

                Assert.Equal(30, items[0]["cost"]);
                Assert.Equal(28, items[1]["cost"]);
                Assert.Equal(6, items[2]["cost"]);
            }
        }

        [Fact]
        public void SortedAggregateCostPerProduct()
        {
            using (SortedCostPerProductAggregation aggregation = new SortedCostPerProductAggregation())
            {
                IEnumerable<Row> result = aggregation.Execute(_rows);
                List<Row> items = new List<Row>(result);
                Assert.Equal(4, items.Count);
                Assert.Equal("milk", items[0]["name"]);
                Assert.Equal("sugar", items[1]["name"]);
                Assert.Equal("coffee", items[2]["name"]);
                Assert.Equal("sugar", items[3]["name"]);

                Assert.Equal(30, items[0]["cost"]);
                Assert.Equal(25, items[1]["cost"]);
                Assert.Equal(6, items[2]["cost"]);
                Assert.Equal(3, items[3]["cost"]);
            }
        }
    }
}