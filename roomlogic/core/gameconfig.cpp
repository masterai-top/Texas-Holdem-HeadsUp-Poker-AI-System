#include "common/macros.h"
#include "gameroot.h"
#include "logic/roomlogic/core/gameconfig.h"
#include "config/gameconfig.h"
#include "process/process.h"
#include "context/context.h"
#include "utils/tarslog.h"

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            void GameConfig(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"GameConfig.");

                using namespace config;
                using namespace context;
                using namespace process;

                if ( root->pro->getProcess() != nil_nnstate)
                {
                    //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"GameConfig. game is playing.");
                    return;
                }

                //
                root->cfg->loadRoomConfig(p);
                root->cfg->printRoomConfig();

                //
                root->con->gameInit();
                root->pro->gameInit();

                __CATCH__
                PERFSTATS_EXIT();
            }
        }
    }
}
