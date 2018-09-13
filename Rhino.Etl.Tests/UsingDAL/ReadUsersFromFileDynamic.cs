#if FEATURE_FILEHELPERS_DYNAMIC
namespace Rhino.Etl.Tests.UsingDAL
{
    using System;
    using System.Collections.Generic;
    using FileHelpers;
    using FileHelpers.Dynamic;
    using Rhino.Etl.Core;
    using Rhino.Etl.Core.Operations;

    public class ReadUsersFromFileDynamic : AbstractOperation
    {
        private Type _tblClass;
        public ReadUsersFromFileDynamic()
        {
            var userRecordClassBuilder = new DelimitedClassBuilder("UserRecord","\t");
            userRecordClassBuilder.IgnoreFirstLines = 1;
            userRecordClassBuilder.AddField("Id", typeof(Int32));
            userRecordClassBuilder.AddField("Name", typeof(String));
            userRecordClassBuilder.AddField("Email", typeof(String));
            _tblClass = userRecordClassBuilder.CreateRecordClass();
        }

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            var file = new FileHelperEngine(_tblClass);
            //var ary = new[] {"one", "two", "three"};
            //var items = from a in ary select a;
            var items = file.ReadFile("users.txt");
            foreach (object obj in items)
            {
                yield return Row.FromObject(obj);
            }
        }
    }
}
#endif