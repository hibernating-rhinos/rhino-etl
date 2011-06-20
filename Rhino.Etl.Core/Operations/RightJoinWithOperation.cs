using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.Etl.Core.Enumerables;

namespace Rhino.Etl.Core.Operations
{
    ///<summary>
    /// Will take the left side from current pipeline and the right side from contructor argument
    ///</summary>
    public abstract class RightJoinWithOperation : JoinOperation
    {
        ///<summary>
        ///</summary>
        ///<param name="rightOp"></param>
        protected RightJoinWithOperation(IOperation rightOp)
        {
            right.Register(rightOp);
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> leftRows)
        {
            PrepareForJoin();

            IEnumerable<Row> rightEnumerable = GetRightEnumerable();

            IEnumerable<Row> execute = left.Execute(leftRows);
            foreach (Row leftRow in new EventRaisingEnumerator(left, execute))
            {
                ObjectArrayKeys key = leftRow.CreateKey(leftColumns);
                List<Row> rightRows;
                if (this.rightRowsByJoinKey.TryGetValue(key, out rightRows))
                {
                    foreach (Row rightRow in rightRows)
                    {
                        rightRowsWereMatched[rightRow] = null;
                        yield return MergeRows(leftRow, rightRow);
                    }
                }
                else if ((jointype & JoinType.Left) != 0)
                {
                    Row emptyRow = new Row();
                    yield return MergeRows(leftRow, emptyRow);
                }
                else
                {
                    LeftOrphanRow(leftRow);
                }
            }
            foreach (Row rightRow in rightEnumerable)
            {
                if (rightRowsWereMatched.ContainsKey(rightRow))
                    continue;
                Row emptyRow = new Row();
                if ((jointype & JoinType.Right) != 0)
                    yield return MergeRows(emptyRow, rightRow);
                else
                    RightOrphanRow(rightRow);
            }
        }

    }
}
