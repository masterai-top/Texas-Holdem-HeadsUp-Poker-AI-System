#include "common/macros.h"
#include "gameroot.h"
#include "logic/roomlogic/core/userconfig.h"
#include "context/context.h"
#include "Comm/ITableGame.h"
#include "context/user.h"
#include "config/gameconfig.h"
#include "utils/tarslog.h"
#include "message/sendroommessage.h"
#include "message/sendclientmessage.h"
#include "logic/clientlogic/core/onready.h"
#include "process/process.h"
#include "ddz.pb.h"

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            void UserConfig(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                using namespace RoomSo;
                using namespace context;
                using namespace config;
                using namespace message;
                using namespace clientlogic;

                //
                RoomSo::TGAME_UserConfig const &userconfig = *static_cast<RoomSo::TGAME_UserConfig const *>(p);

                DLOG_TRACE("roomid:" << root->roomid() << ", " << "TGAME_UserConfig_E" << ", uid: " << userconfig.lPlayerID << ", ltakeCoin: " << userconfig.ltakeCoin
                           << ", bAutoSupplement: " << userconfig.bAutoSupplement << ", lminTakeCoin: " << userconfig.lminTakeCoin << ", lmaxTakeCoin: " << userconfig.lmaxTakeCoin);

                //玩家
                User *user = root->con->getUserByUid(userconfig.lPlayerID);
                if (user == NULL)
                {
                    DLOG_TRACE( "user not exist, uid: " << userconfig.lPlayerID);
                    //return;
                }

                if (NULL != user)
                {
                    if(user->isRobot()) //机器人自动准备
                    {
                        XGameDDZProto::DDZ_msg2csReady shcm;
                        shcm.set_icid(user->getCid());
                        vector<char> vecOutBuffer;
                        pbTobuffer(shcm, vecOutBuffer);
                        clientlogic::OnReady(user->getUid(), vecOutBuffer, root);
                    }
                    DLOG_TRACE("roomid:" << root->roomid() << ", " << " uid: " << userconfig.lPlayerID << ", left:" << user->isLeft());
                }

                __CATCH__
                PERFSTATS_EXIT();
            }
        }
    }
}
