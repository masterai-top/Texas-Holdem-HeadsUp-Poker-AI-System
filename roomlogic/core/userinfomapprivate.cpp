#include "common/macros.h"
#include "gameroot.h"
#include "context/context.h"
#include "Comm/ITableGame.h"
#include "utils/tarslog.h"

namespace game
{
    namespace logic
    {
        namespace roomlogic
        {
            void UserInfoMapPrivate(void const *p, GameRoot *root)
            {
                PERFSTATS_ENTRY();
                __TRY__
                using namespace RoomSo;
                using namespace context;

                TGAME_UserInfoMapPrivate const *nnrs = static_cast<TGAME_UserInfoMapPrivate const *>(p);
                DLOG_TRACE("roomid:"<<root->roomid()<<", "<<"TGAME_UserInfoMapPrivate_E nnrs:"<< printTars(*nnrs));

                for (auto it = nnrs->userInfoMap.begin(); it != nnrs->userInfoMap.end(); it++)
                {
                    //
                    User *user = root->con->getUserByUid(it->first);
                    if (user != NULL)
                    {
                        user->setNick(it->second.sNick);
                        user->setUrl(it->second.sUrl);
                        user->setGender(it->second.iGender);
                    }
                }
                __CATCH__
                PERFSTATS_EXIT();
            }
        }
    }
}
