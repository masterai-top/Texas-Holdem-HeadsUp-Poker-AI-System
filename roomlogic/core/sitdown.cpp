#include "common/macros.h"
#include "common/nndef.h"
#include "gameroot.h"
#include "logic/roomlogic/core/usersitdown.h"
#include "context/context.h"
#include "Comm/ITableGame.h"
#include "config/gameconfig.h"
#include "utils/tarslog.h"
#include "process/process.h"
#include "logic/gamelogic/core/checkbegin.h"
#include "logic/clientlogic/core/gamestation.h"
#include "context/context.h"
#include "message/sendclientmessage.h"

using namespace nndef;

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            int SitDown(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                using namespace RoomSo;
                using namespace config;
                using namespace process;
                using namespace gamelogic;
                using namespace clientlogic;
                using namespace context;
                using namespace message;

                __CATCH__
                PERFSTATS_EXIT();
                return 0;
            }
        }
    }
}
