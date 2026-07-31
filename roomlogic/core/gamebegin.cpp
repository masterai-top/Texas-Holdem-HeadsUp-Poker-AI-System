#include "common/macros.h"
#include "gameroot.h"
#include "logic/roomlogic/core/gamebegin.h"
#include "context/context.h"
#include "utils/tarslog.h"

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            void GameBegin(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                using namespace context;

                //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"GameBegin.");

                __CATCH__
                PERFSTATS_EXIT();
            }
        }
    }
}
