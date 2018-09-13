namespace Rhino.Etl.Tests.Util
{
    using System;
    using System.Collections.Generic;
    using NSubstitute;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.DataReaders;
    using Xunit;

    public class DictionaryEnumeratorDataReaderFixture
    {
        [Fact]
        public void WillDisposeInternalEnumeratorAndEnumerableWhenDisposed()
        {
            IEnumerable<Row> enumerable = Substitute.For<IEnumerable<Row>, IDisposable>();
            IEnumerator<Row> enumerator = Substitute.For<IEnumerator<Row>>();
            enumerable.GetEnumerator().Returns(enumerator);

            DictionaryEnumeratorDataReader reader =
                new DictionaryEnumeratorDataReader(new Dictionary<string, Type>(), enumerable);
            reader.Dispose();

            enumerator.Received(1).Dispose();
            ((IDisposable)enumerable).Received(1).Dispose();
        }
    }
}