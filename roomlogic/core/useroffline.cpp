#include "common/macros.h"
#include "gameroot.h"
#include "logic/roomlogic/core/useroffline.h"
#include "context/context.h"
#include "Comm/ITableGame.h"
#include "utils/tarslog.h"
#include "message/sendclientmessage.h"
#include "ddz.pb.h"

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            void UserOffline(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__

                //DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"UserOffline.");

                using namespace RoomSo;
                using namespace context;
                using namespace message;

                TGAME_UserOffline const *nnrs = static_cast<TGAME_UserOffline const *>(p);
                DLOG_TRACE("roomid:"<<root->roomid()<<", TGAME_UserOffline uid: " << nnrs->lPlayerID );

                User *user = root->con->getUserByUid(nnrs->lPlayerID);
                if (user == NULL)
                {
                    return;
                }

                user->setTuoGuan(true);

                XGameDDZProto::DDZ_msg2csTuoGuan tg;
                tg.set_iresultid(0);
                tg.set_btuoguan(user->isTuoGuan());
                tg.set_icid(user->getCid());
                sendAllClientMessage<XGameDDZProto::DDZ_msg2csTuoGuan>(XGameDDZProto::DDZ_msg2csTuoGuan_E, tg, root);

                __CATCH__
                PERFSTATS_EXIT();
            }
        }
    }
}
