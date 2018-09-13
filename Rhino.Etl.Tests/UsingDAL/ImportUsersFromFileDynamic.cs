#if FEATURE_FILEHELPERS_DYNAMIC
namespace Rhino.Etl.Tests.UsingDAL
{
    using Rhino.Etl.Core;

    public class ImportUsersFromFileDynamic : EtlProcess
    {
        protected override void Initialize()
        {
            Register(new ReadUsersFromFileDynamic());
            Register(new SaveToDal());
        }
    }
}
#endif