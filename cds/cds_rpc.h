#ifndef __CDS_RPC_H__
#define __CDS_RPC_H__


static inline void io_init(io_t *io, const chkid_t *chkid,
                           uint32_t size, uint64_t offset, uint32_t flags)
{
        memset(io, 0 ,sizeof(io_t));

        if(chkid)
                io->id = *chkid;

        io->offset = offset;
        io->size = size;
        io->flags = flags;
        io->lsn = 0;
        io->lease = -1;
        io->snapvers = 0;
}

int cds_rpc_init();
int cds_rpc_read(const nid_t *nid, const io_t *io, buffer_t *_buf);
int cds_rpc_write(const nid_t *nid, const io_t *io, const buffer_t *_buf);

#endif
