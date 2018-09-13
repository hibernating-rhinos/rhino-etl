namespace Rhino.Etl.Tests.Joins
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using NSubstitute;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;
    using Xunit;

    public class JoinEventsFixture
    {
        public Action<IOperation, Row> processAction = delegate { };
        public Action<IOperation> finishedAction = delegate { };

        [Fact]
        public void CanPassOnAddedProcessedEvents()
        {
            //Arrange
            var join = new TestAbstractJoinOperation();
            var op1 = Substitute.For<IOperation>();
            var op2 = Substitute.For<IOperation>();
            join.Left(op1).Right(op2);

            //Act
            join.OnRowProcessed += processAction;

            //Assert
            op1.Received(1).OnRowProcessed += processAction;
            op2.Received(1).OnRowProcessed += processAction;

            Assert.Equal(2, GetEventHandlers(join, nameof(join.OnRowProcessed)).Length);
        }

        [Fact]
        public void CanPassOnAddedFinishedEvents()
        {
            var join = new TestAbstractJoinOperation();
            var op1 = Substitute.For<IOperation>();
            var op2 = Substitute.For<IOperation>();
            join.Left(op1).Right(op2);

            join.OnFinishedProcessing += finishedAction;

            op1.Received().OnFinishedProcessing += finishedAction;
            op2.Received().OnFinishedProcessing += finishedAction;

            Assert.Equal(2, GetEventHandlers(join, nameof(join.OnFinishedProcessing)).Length);
        }

        [Fact]
        public void CanPassOnRemovedProcessedEvents()
        {
            //Arrange
            var join = new TestAbstractJoinOperation();
            var op1 = Substitute.For<IOperation>();
            var op2 = Substitute.For<IOperation>();
            join.Left(op1).Right(op2);

            //Act
            join.OnRowProcessed += processAction;
            join.OnRowProcessed -= processAction;

            //Assert
            op1.Received(1).OnRowProcessed += processAction;
            op1.Received(1).OnRowProcessed -= processAction;
            op2.Received(1).OnRowProcessed += processAction;
            op2.Received(1).OnRowProcessed -= processAction;

            Assert.Single(GetEventHandlers(join, nameof(join.OnRowProcessed)));
        }

        [Fact]
        public void CanPassOnRemovedFinishedEvents()
        {
            var join = new TestAbstractJoinOperation();
            var op1 = Substitute.For<IOperation>();
            var op2 = Substitute.For<IOperation>();
            join.Left(op1).Right(op2);

            join.OnFinishedProcessing += finishedAction;
            join.OnFinishedProcessing -= finishedAction;

            op1.Received(1).OnFinishedProcessing += finishedAction;
            op1.Received(1).OnFinishedProcessing -= finishedAction;
            op2.Received(1).OnFinishedProcessing += finishedAction;
            op2.Received(1).OnFinishedProcessing -= finishedAction;

            Assert.Single(GetEventHandlers(join, nameof(join.OnFinishedProcessing)));
        }

        private static Delegate[] GetEventHandlers(object instance, string eventName)
        {
            var field = typeof(AbstractOperation).GetField(eventName, BindingFlags.Static | BindingFlags.Instance | BindingFlags.NonPublic);
            var eventHandler = (Delegate)field.GetValue(instance);
            return eventHandler.GetInvocationList();
        }
    }

    public class TestAbstractJoinOperation : JoinOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            throw new NotImplementedException();
        }

        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            throw new NotImplementedException();
        }

        protected override void SetupJoinConditions()
        {
            throw new NotImplementedException();
        }
    }
}
