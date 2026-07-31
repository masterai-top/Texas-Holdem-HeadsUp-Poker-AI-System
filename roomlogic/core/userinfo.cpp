#include "common/macros.h"
#include "gameroot.h"
#include "context/context.h"
#include "Comm/ITableGame.h"
#include "utils/tarslog.h"
#include "message/sendclientmessage.h"
#include "logic/gamelogic/core/checkbegin.h"
#include "process/process.h"

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            void UserInfo(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"UserInfo.");

                using namespace RoomSo;
                using namespace context;
                using namespace message;
                using namespace gamelogic;
                using namespace process;

                 //
                TGAME_UserInfo const *nnrs = static_cast<TGAME_UserInfo const *>(p);
                if (nnrs == NULL)
                {
                    RLOG_DEBUG << "user info null." << endl;
                    return;
                }

                //
                DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"TGAME_UserInfo_E" << ", uid: " << nnrs->lPlayerID << ", iType: " << nnrs->iType << ", lPlayerWealth: " << nnrs->lPlayerWealth);

                User *user = root->con->getUserByUid(nnrs->lPlayerID);
                if (user)
                {
                    //设置货币
                    if (nnrs->iType == 4)
                    {
                        user->setWealth(nnrs->lPlayerWealth);
                    }
                }
                __CATCH__
                PERFSTATS_EXIT();
            }
        }
    }
}
