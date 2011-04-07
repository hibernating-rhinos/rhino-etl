using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using Rhino.Mocks;
using Xunit;

namespace Rhino.Etl.Tests
{
    public class PartialProcessEventsFixture
    {

        [Fact]
        public void CanPassOnPrepareForExecutionEvents()
        {
            //Arrange
            var process = new PartialProcessOperation();
            var pe = MockRepository.GenerateMock<IPipelineExecuter>();
            const int nOps = 5;
            var ops = new IOperation[nOps];
            for (var i = 0; i < nOps; i++)
            {
                ops[i] = MockRepository.GenerateMock<IOperation>();
                ops[i].Expect(x => x.PrepareForExecution(pe));
                process.Register(ops[i]);
            }

            //Act
            process.PrepareForExecution(pe);

            //Assert
            foreach (var op in ops)
                op.VerifyAllExpectations();
        }
    }
}
