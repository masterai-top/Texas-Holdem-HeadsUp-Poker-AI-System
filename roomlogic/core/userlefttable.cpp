#include "Comm/ITableGame.h"
#include "common/macros.h"
#include "common/nndef.h"
#include "utils/tarslog.h"
#include "gameroot.h"
#include "logic/roomlogic/core/userlefttable.h"
#include "logic/gamelogic/core/leavedesk.h"
#include "context/context.h"
#include "process/process.h"
#include "message/sendroommessage.h"
#include "logic/gamelogic/core/endtimer.h"
#include "config/gameconfig.h"
#include "suoha.pb.h"

using namespace nndef;

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {

            using namespace RoomSo;
            using namespace context;
            using namespace process;
            using namespace gamelogic;
            using namespace message;
            using namespace config;

            int UserLeftTable(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                TGAME_UserLeftTable const *nnrs = static_cast<TGAME_UserLeftTable const *>(p);
                User *user = root->con->getUserByUid(nnrs->lPlayerID);

                if (nnrs == NULL || user == NULL)
                {
                    DLOG_TRACE("roomid:" << root->roomid());
                    return 0;
                }

                DLOG_TRACE("roomid:" << root->roomid() << ", user left uid:" << nnrs->lPlayerID << ", cid: "<< user->getCid() <<", iType: "<< nnrs->iType );     
                user->setLeft(true);
                user->setSelfLevel(nnrs->iType == 2);

                if(root->pro->getProcess() == nil_nnstate && nnrs->iType > 1)
                {
                    gamelogic::LeaveDesk(root);
                }
                
                __CATCH__
                PERFSTATS_EXIT();
                return 0;
            }
        }
    }
}
