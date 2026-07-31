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
#include "message/sendroommessage.h"

using namespace nndef;

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            int UserSitDown(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"UserSitDown roomid:" << root->roomid());

                using namespace RoomSo;
                using namespace config;
                using namespace process;
                using namespace gamelogic;
                using namespace clientlogic;
                using namespace context;
                using namespace message;

                TGAME_UserSitDown const *nnrs = static_cast<TGAME_UserSitDown const *>(p);
                DLOG_TRACE("roomid:" << root->roomid() << ", " << "TGAME_UserSitDown_E" << ", uid: " << nnrs->lPlayerID << ", iChairID: " << nnrs->iChairID << ", iRobot: " << nnrs->iRobot);

                //该位置没有人
                User *oldUser = root->con->getUserByCid(nnrs->iChairID);
                if (!oldUser)
                {
                    //添加玩家
                    root->con->addUser(nnrs->lPlayerID, nnrs->iChairID, nnrs->iRobot != 0);

                    DLOG_TRACE("roomid:" << root->roomid() << ", " << "add user , cid = " << nnrs->iChairID << ", uid: " << nnrs->lPlayerID);
                    //玩家数据
                    User *user = root->con->getUserByCid(nnrs->iChairID);
                    if (!user)
                    {
                        //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<" user not exist, uid: " << nnrs->lPlayerID);
                        return -2;
                    }

                    user->setRobot(nnrs->iRobot, nnrs->lRobotID);

                    DLOG_TRACE("roomid:" << root->roomid() << ", " << " uid: " << nnrs->lPlayerID << ", left:" << user->isLeft());
                }
                
                __CATCH__
                PERFSTATS_EXIT();
                return 0;
            }
        }
    }
}
