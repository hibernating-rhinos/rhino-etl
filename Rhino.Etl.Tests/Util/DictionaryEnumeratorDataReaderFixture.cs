namespace Rhino.Etl.Tests.Util
{
    using System;
    using System.Collections.Generic;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.DataReaders;
    using Rhino.Mocks;
    using Xunit;


    [Collection("Rhino.Mocks")]
    public class DictionaryEnumeratorDataReaderFixture
    {
        [Fact]
        public void WillDisposeInternalEnumeratorAndEnumerableWhenDisposed()
        {
            MockRepository mocks = new MockRepository();
            IEnumerable<Row> enumerable = mocks.DynamicMultiMock<IEnumerable<Row>>(typeof(IDisposable));
            IEnumerator<Row> enumerator = mocks.DynamicMock<IEnumerator<Row>>();
            using(mocks.Record())
            {
                SetupResult.For(enumerable.GetEnumerator()).Return(enumerator);
                enumerator.Dispose();
                ((IDisposable)enumerable).Dispose();
            }
            using (mocks.Playback())
            {
                DictionaryEnumeratorDataReader reader =
                    new DictionaryEnumeratorDataReader(new Dictionary<string, Type>(), enumerable);
                reader.Dispose();
            }
        }
    }
}