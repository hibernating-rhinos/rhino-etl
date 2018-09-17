namespace Rhino.Etl.Tests.Branches
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using NSubstitute;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;
    using Xunit;

    public class BranchEventsFixture
    {
        private readonly Action<IOperation, Row> _processAction = delegate { };
        private readonly Action<IOperation> _finishedAction = delegate { };

        [Fact]
        public void CanPassOnAddedProcessedEvents()
        {
            //Arrange
            var branching = new TestAbstractBranchingOperation();
            const int nOps = 5;
            var ops = new IOperation[nOps];
            for (var i = 0; i < nOps; i++)
            {
                ops[i] = Substitute.For<IOperation>();
                branching.Add(ops[i]);
            }

            //Act
            branching.OnRowProcessed += _processAction;

            //Assert
            foreach (var op in ops)
            {
                op.Received(1).OnRowProcessed += _processAction;
            }

            Assert.Equal(2, GetEventHandlers(branching, nameof(branching.OnRowProcessed)).Length);
        }

        [Fact]
        public void CanPassOnAddedFinishedEvents()
        {
            var branching = new TestAbstractBranchingOperation();
            const int nOps = 5;
            var ops = new IOperation[nOps];
            for (var i = 0; i < nOps; i++)
            {
                ops[i] = Substitute.For<IOperation>();
                branching.Add(ops[i]);
            }

            branching.OnFinishedProcessing += _finishedAction;

            foreach (var op in ops)
            {
                op.Received(1).OnFinishedProcessing += _finishedAction;
            }

            Assert.Equal(2, GetEventHandlers(branching, nameof(branching.OnFinishedProcessing)).Length);
        }

        [Fact]
        public void CanPassOnRemovedProcessedEvents()
        {
            //Arrange
            var branching = new TestAbstractBranchingOperation();
            const int nOps = 5;
            var ops = new IOperation[nOps];
            for (var i = 0; i < nOps; i++)
            {
                ops[i] = Substitute.For<IOperation>();
                branching.Add(ops[i]);
            }

            //Act
            branching.OnRowProcessed += _processAction;
            branching.OnRowProcessed -= _processAction;

            //Assert
            foreach (var op in ops)
            {
                op.Received(1).OnRowProcessed += _processAction;
                op.Received(1).OnRowProcessed -= _processAction;
            }

            Assert.Single(GetEventHandlers(branching, nameof(branching.OnRowProcessed)));
        }

        [Fact]
        public void CanPassOnRemovedFinishedEvents()
        {
            var branching = new TestAbstractBranchingOperation();
            const int nOps = 5;
            var ops = new IOperation[nOps];
            for (var i = 0; i < nOps; i++)
            {
                ops[i] = Substitute.For<IOperation>();
                branching.Add(ops[i]);
            }

            branching.OnFinishedProcessing += _finishedAction;
            branching.OnFinishedProcessing -= _finishedAction;

            foreach (var op in ops)
            {
                op.Received(1).OnFinishedProcessing += _finishedAction;
                op.Received(1).OnFinishedProcessing -= _finishedAction;
            }

            Assert.Single(GetEventHandlers(branching, nameof(branching.OnFinishedProcessing)));
        }

        private static Delegate[] GetEventHandlers(object instance, string eventName)
        {
            var field = typeof(AbstractOperation).GetField(eventName, BindingFlags.Static | BindingFlags.Instance | BindingFlags.NonPublic);
            var eventHandler = (Delegate)field.GetValue(instance);
            return eventHandler.GetInvocationList();
        }
    }

    public class TestAbstractBranchingOperation : AbstractBranchingOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            throw new NotImplementedException();
        }
    }
}
