using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.Etl.Core.Enumerables;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class MultiJoinOperation : AbstractOperation
    {
        /// <summary>
        /// 
        /// </summary>
        protected readonly PartialProcessOperation left = new PartialProcessOperation();
        /// <summary>
        /// 
        /// </summary>
        protected readonly PartialProcessOperation right = new PartialProcessOperation();
        /// <summary>
        /// 
        /// </summary>
        protected JoinType jointype;
        /// <summary>
        /// 
        /// </summary>
        protected string[] leftColumns;
        /// <summary>
        /// 
        /// </summary>
        protected string[][] leftColumnsSet;
        /// <summary>
        /// 
        /// </summary>
        protected string[] rightColumns;
        /// <summary>
        /// 
        /// </summary>
        protected string[][] rightColumnsSet;
        /// <summary>
        /// 
        /// </summary>
        protected Dictionary<Row, object> rightRowsWereMatched = new Dictionary<Row, object>();
        /// <summary>
        /// 
        /// </summary>
        protected Dictionary<ObjectArrayKeys, List<Row>> rightRowsByJoinKey = new Dictionary<ObjectArrayKeys, List<Row>>();

        /// <summary>
        /// 
        /// </summary>
        protected MultiJoinBuilder builder = new MultiJoinBuilder();

        /// <summary>
        /// Sets the right part of the join
        /// </summary>
        /// <value>The right.</value>
        public MultiJoinOperation Right(IOperation value)
        {
            right.Register(value);
            return this;
        }


        /// <summary>
        /// Sets the left part of the join
        /// </summary>
        /// <value>The left.</value>
        public MultiJoinOperation Left(IOperation value)
        {
            left.Register(value);
            return this;
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="ignored">Ignored rows</param>
        /// <returns></returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> ignored)
        {
            PrepareForJoin();

            IEnumerable<Row> rightEnumerable = GetRightEnumerable();

            IEnumerable<Row> execute = left.Execute(null);
            foreach (Row leftRow in new EventRaisingEnumerator(left, execute))
            {
                for (int i = 0; i < builder.Joins.Length; i++)
                {
                    var join = builder.Joins[i];
                    ObjectArrayKeys key = leftRow.CreateKey(join.LeftColumns);
                    //ObjectArrayKeysSet key = leftRow.CreateKey(leftColumnsSet);
                    List<Row> rightRows;
                    if (this.rightRowsByJoinKey.TryGetValue(key, out rightRows))
                    {
                        foreach (Row rightRow in rightRows)
                        {
                            rightRowsWereMatched[rightRow] = null;
                            yield return MergeRows(leftRow, rightRow);
                        }
                        break;
                    }
                    else if (i < builder.Joins.Length -1)
                    {
                        continue;
                    }
                    else if ((join.JoinType & JoinType.Left) != 0)
                    {
                        Row emptyRow = new Row();
                        yield return MergeRows(leftRow, emptyRow);
                    }
                    else
                    {
                        LeftOrphanRow(leftRow);
                    }
                }
            }
            foreach (Row rightRow in rightEnumerable)
            {
                if (rightRowsWereMatched.ContainsKey(rightRow))
                    continue;
                Row emptyRow = new Row();
                //if ((jointype & JoinType.Right) != 0)
                yield return MergeRows(emptyRow, rightRow);
                //else
                //    RightOrphanRow(rightRow);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        protected void PrepareForJoin()
        {
            Initialize();

            Guard.Against(left == null, "Left branch of a join cannot be null");
            Guard.Against(right == null, "Right branch of a join cannot be null");

            SetupJoinConditions();

            leftColumnsSet = new string[builder.Joins.Length][];
            rightColumnsSet = new string[builder.Joins.Length][];
            for (int i = 0; i < builder.Joins.Length; i++)
            {
                var join = builder.Joins[i];
                leftColumnsSet[i] = join.LeftColumns;
                rightColumnsSet[i] = join.RightColumns;
                //leftColumns = join.LeftColumns;
                //rightColumns = join.RightColumns;
            }
            //Guard.Against(leftColumns == null, "You must setup the left columns");
            //Guard.Against(rightColumns == null, "You must setup the right columns");
            Guard.Against(leftColumnsSet == null || leftColumnsSet.Length == 0, "You must setup the left columns");
            Guard.Against(rightColumnsSet == null || rightColumnsSet.Length == 0, "You must setup the right columns");
        }

        /// <summary>
        /// 
        /// </summary>
        protected IEnumerable<Row> GetRightEnumerable()
        {
            IEnumerable<Row> rightEnumerable = new CachingEnumerable<Row>(
                new EventRaisingEnumerator(right, right.Execute(null))
                );
            foreach (Row row in rightEnumerable)
            {
                foreach (var join in builder.Joins)
                {
                    ObjectArrayKeys key = row.CreateKey(join.RightColumns);
                    //ObjectArrayKeysSet key = row.CreateKey(rightColumnsSet);
                    List<Row> rowsForKey;
                    if (this.rightRowsByJoinKey.TryGetValue(key, out rowsForKey) == false)
                    {
                        this.rightRowsByJoinKey[key] = rowsForKey = new List<Row>();
                    }
                    rowsForKey.Add(row);
                }
            }
            return rightEnumerable;
        }

        /// <summary>
        /// Called when a row on the right side was filtered by
        /// the join condition, allow a derived class to perform 
        /// logic associated to that, such as logging
        /// </summary>
        protected virtual void RightOrphanRow(Row row)
        {

        }

        /// <summary>
        /// Called when a row on the left side was filtered by
        /// the join condition, allow a derived class to perform 
        /// logic associated to that, such as logging
        /// </summary>
        /// <param name="row">The row.</param>
        protected virtual void LeftOrphanRow(Row row)
        {

        }

        /// <summary>
        /// Merges the two rows into a single row
        /// </summary>
        /// <param name="leftRow">The left row.</param>
        /// <param name="rightRow">The right row.</param>
        /// <returns></returns>
        protected abstract Row MergeRows(Row leftRow, Row rightRow);

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected virtual void Initialize()
        {
        }

        /// <summary>
        /// Setups the join conditions.
        /// </summary>
        protected abstract void SetupJoinConditions();

        /// <summary>
        /// Create a multi join
        /// </summary>
        public MultiJoinBuilder MultiJoin(MultiJoinItemBuilder joinBuilder)
        {
            builder.And(joinBuilder);
            return builder;
        }
        /// <summary>
        /// Create an inner join
        /// </summary>
        /// <value>The inner.</value>
        protected MultiJoinItemBuilder InnerJoin
        {
            get
            {
                var joinBuilder = new MultiJoinItemBuilder(this, JoinType.Inner);
                //builder.And(joinBuilder);
                return joinBuilder;
            }
        }

        /// <summary>
        /// Create a left outer join
        /// </summary>
        /// <value>The inner.</value>
        protected MultiJoinItemBuilder LeftJoin
        {
            get
            {
                //return new MultiJoinItemBuilder(this, JoinType.Left);
                var joinBuilder = new MultiJoinItemBuilder(this, JoinType.Left);
                //builder.And(joinBuilder);
                return joinBuilder;
            }
        }


        /// <summary>
        /// Create a right outer join
        /// </summary>
        /// <value>The inner.</value>
        protected MultiJoinItemBuilder RightJoin
        {
            get
            {
                //return new MultiJoinItemBuilder(this, JoinType.Right);
                var joinBuilder = new MultiJoinItemBuilder(this, JoinType.Right);
                builder.And(joinBuilder);
                return joinBuilder;
            }
        }


        /// <summary>
        /// Create a full outer join
        /// </summary>
        /// <value>The inner.</value>
        protected MultiJoinItemBuilder FullOuterJoin
        {
            get
            {
                //return new MultiJoinItemBuilder(this, JoinType.Full);
                var joinBuilder = new MultiJoinItemBuilder(this, JoinType.Full);
                builder.And(joinBuilder);
                return joinBuilder;
            }
        }


        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            left.Dispose();
            right.Dispose();
        }


        /// <summary>
        /// Initializes this instance
        /// </summary>
        /// <param name="pipelineExecuter">The current pipeline executer.</param>
        public override void PrepareForExecution(IPipelineExecuter pipelineExecuter)
        {
            left.PrepareForExecution(pipelineExecuter);
            right.PrepareForExecution(pipelineExecuter);
        }

        /// <summary>
        /// Gets all errors that occured when running this operation
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<Exception> GetAllErrors()
        {
            foreach (Exception error in left.GetAllErrors())
            {
                yield return error;
            }
            foreach (Exception error in right.GetAllErrors())
            {
                yield return error;
            }
        }




        /// <summary>
        /// Fluent interface to create joins
        /// </summary>
        public class MultiJoinItemBuilder
        {
            private readonly MultiJoinOperation parent;
            private string[] leftColumns;
            private string[] rightColumns;
            private JoinType joinType;

            /// <summary>
            /// Initializes a new instance of the <see cref="MultiJoinBuilder"/> class.
            /// </summary>
            /// <param name="parent">The parent.</param>
            /// <param name="joinType">Type of the join.</param>
            public MultiJoinItemBuilder(MultiJoinOperation parent, JoinType joinType)
            {
                this.parent = parent;
                parent.jointype = joinType;
                this.joinType = joinType;
            }

            /// <summary>
            /// Setup the left side of the join
            /// </summary>
            /// <param name="columns">The columns.</param>
            /// <returns></returns>
            public MultiJoinItemBuilder Left(params string[] columns)
            {
                //parent.leftColumns = columns;
                this.leftColumns = columns;
                return this;
            }

            /// <summary>
            /// Setup the right side of the join
            /// </summary>
            /// <param name="columns">The columns.</param>
            /// <returns></returns>
            public MultiJoinItemBuilder Right(params string[] columns)
            {
                //parent.rightColumns = columns;
                this.rightColumns = columns;
                return this;
            }

            /// <summary>
            /// 
            /// </summary>
            public string[] LeftColumns
            {
                get { return leftColumns; }
            }

            /// <summary>
            /// 
            /// </summary>
            public string[] RightColumns
            {
                get { return rightColumns; }
            }

            /// <summary>
            /// 
            /// </summary>
            public JoinType JoinType
            {
                get { return joinType; }
            }
        }

        /// <summary>
        /// Fluent interface to create joins
        /// </summary>
        public class MultiJoinBuilder
        {
            private readonly MultiJoinOperation parent;
            private List<MultiJoinItemBuilder> joins;
            /// <summary>
            /// Initializes a new instance of the <see cref="MultiJoinBuilder"/> class.
            /// </summary>
            /// <param name="parent">The parent.</param>
            /// <param name="joinType">Type of the join.</param>
            public MultiJoinBuilder(MultiJoinOperation parent, JoinType joinType)
            {
                this.parent = parent;
                this.joins = new List<MultiJoinItemBuilder> { new MultiJoinItemBuilder(parent, joinType) };
            }

            /// <summary>
            /// 
            /// </summary>
            public MultiJoinBuilder()
            {
                //this.parent = parent;
                this.joins = new List<MultiJoinItemBuilder>();
            }
            ///// <summary>
            ///// Setup the left side of the join
            ///// </summary>
            ///// <param name="columns">The columns.</param>
            ///// <returns></returns>
            //public MultiJoinBuilder Left(params string[] columns)
            //{
            //    parent.leftColumns = columns;
            //    return this;
            //}

            ///// <summary>
            ///// Setup the right side of the join
            ///// </summary>
            ///// <param name="columns">The columns.</param>
            ///// <returns></returns>
            //public MultiJoinBuilder Right(params string[] columns)
            //{
            //    parent.rightColumns = columns;
            //    return this;
            //}

            ///// <summary>
            ///// Create an inner join
            ///// </summary>
            ///// <value>The inner.</value>
            //protected MultiJoinItemBuilder OrLeftJoin
            //{
            //    get
            //    {
            //        var newJoin = new MultiJoinItemBuilder(parent, JoinType.Left);
            //        joins.Add(newJoin);
            //        return newJoin;
            //    }
            //}

            /// <summary>
            /// 
            /// </summary>
            public MultiJoinBuilder Or(MultiJoinItemBuilder multiJoinItemBuilder)
            {
                joins.Add(multiJoinItemBuilder);
                return this;
            }
            /// <summary>
            /// 
            /// </summary>
            public MultiJoinBuilder And(MultiJoinItemBuilder multiJoinItemBuilder)
            {
                joins.Add(multiJoinItemBuilder);
                return this;
            }
            /// <summary>
            /// 
            /// </summary>
            public MultiJoinItemBuilder[] Joins
            {
                get { return joins.ToArray(); }
            }

        }
    }
}
