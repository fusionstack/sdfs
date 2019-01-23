#include <errno.h>

#define DBG_SUBSYS S_LIBYNET

#include "ynet_net.h"
#include "ynet_rpc.h"
#include "dbg.h"


int ynet_pack_crcsum(buffer_t *pack)
{
        uint32_t crcode;
        ynet_net_head_t *head;

        head = mbuffer_head(pack);

        if (head->crcode)
                return 0;

        crcode = mbuffer_crc(pack, YNET_NET_REQ_OFF, pack->len);

        head->crcode = crcode;

        DBUG("CRC %x len %u type %u prog %u seq %u no %u\n", crcode, head->len,
             head->type, head->prog, head->msgid.figerprint, head->msgid.idx);

        return 0;
}

int ynet_pack_crcverify(buffer_t *pack)
{
        int ret;
        uint32_t crcode;
        ynet_net_head_t head;

        mbuffer_get(pack, &head, sizeof(ynet_net_head_t));

        if (!head.crcode)
                return 0;

        crcode = mbuffer_crc(pack, YNET_NET_REQ_OFF, pack->len);

        if (head.crcode != crcode) {
                DERROR("crc code error %x:%x len %u\n", head.crcode,
                       crcode, pack->len);
                ret = EBADF;
                UNIMPLEMENTED(__DUMP__);
                GOTO(err_ret, ret);
        }

        DBUG("CRC %x len %u type %u prog %u seq %u no %u\n", crcode, head.len,
             head.type, head.prog, head.msgid.figerprint, head.msgid.idx);

        return 0;
err_ret:
        return ret;
}
