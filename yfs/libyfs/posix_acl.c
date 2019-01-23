#include <stdint.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include "posix_acl.h"

#ifndef ACCESSPERMS
#define ACCESSPERMS (S_IRWXU|S_IRWXG|S_IRWXO)
#endif

static int _posix_acl_ea_count(size_t size)
{
        if (size < sizeof(acl_ea_header))
                return -1;
        size -= sizeof(acl_ea_header);
        if (size % sizeof(acl_ea_entry))
                return -1;
        return size / sizeof(acl_ea_entry);
}

size_t posix_acl_ea_size(int count)
{
        return sizeof(acl_ea_header) + count * sizeof(acl_ea_entry);
}

int posix_acl_check(const void *xattr, size_t size)
{
        const acl_ea_header *header;
        if (size < sizeof(*header))
              return -1;

        header = (acl_ea_header *)xattr;
        uint32_t expected_version;
        expected_version = ACL_EA_VERSION;
        if (header->a_version != expected_version)
              return -1;

        const acl_ea_entry *entry = header->a_entries;
        size -= sizeof(*header);
        if (size % sizeof(*entry))
              return -1;

        int count = size / sizeof(*entry);
        if (count == 0)
              return 0;

        int i;
        int state = ACL_USER_OBJ;
        int needs_mask = 0;
        for (i = 0; i < count; ++i) {
              uint16_t tag = entry->e_tag;
              switch(tag) {
              case ACL_USER_OBJ:
                      if (state == ACL_USER_OBJ) {
                              state = ACL_USER;
                              break;
                      }
                      return -1;
              case ACL_USER:
                      if (state != ACL_USER)
                      return -1;
                      needs_mask = 1;
                      break;
              case ACL_GROUP_OBJ:
                      if (state == ACL_USER) {
                              state = ACL_GROUP;
                              break;
                      }
                      return -1;
              case ACL_GROUP:
                      if (state != ACL_GROUP)
                              return -1;
                      needs_mask = 1;
                      break;
              case ACL_MASK:
                      if (state != ACL_GROUP)
                              return -1;
                      state = ACL_OTHER;
                      break;
              case ACL_OTHER:
                      if (state == ACL_OTHER ||
                          (state == ACL_GROUP && !needs_mask)) {
                              state = 0;
                              break;
                      }
              // fall-thru
              default:
                      return -1;
              }
              ++entry;
        }

        return state == 0 ? count : -1;
}

int posix_acl_equiv_mode(const void *xattr, size_t size, mode_t *mode_p)
{
        if (posix_acl_check(xattr, size) < 0)
                return -EINVAL;

        int not_equiv = 0;
        mode_t mode = 0;

        const acl_ea_header *header = (acl_ea_header *)(xattr);
        const acl_ea_entry *entry = header->a_entries;
        int count = (size - sizeof(*header)) / sizeof(*entry);

        int i;
        for (i = 0; i < count; ++i) {
                uint16_t tag = entry->e_tag;
                uint16_t perm = entry->e_perm;
                switch(tag) {
                case ACL_USER_OBJ:
      	                mode |= (perm & S_IRWXO) << 6;
      	                break;
                case ACL_GROUP_OBJ:
                      	mode |= (perm & S_IRWXO) << 3;
                      	break;
                case ACL_OTHER:
                      	mode |= perm & S_IRWXO;
                      	break;
                case ACL_MASK:
      	                mode = (mode & ~S_IRWXG) | ((perm & S_IRWXO) << 3);
      	        /* fall through */
                case ACL_USER:
                case ACL_GROUP:
      	                not_equiv = 1;
      	                break;
                default:
      	                return -EINVAL;
                }
                ++entry;
        }
        if (mode_p)
                *mode_p = (*mode_p & ~ACCESSPERMS) | mode;
        return not_equiv;
}

int posix_acl_default_get(void *acl_buf, size_t acl_buf_size, mode_t mode)
{
        if (_posix_acl_ea_count(acl_buf_size) != ACL_DEFAULT_EA_ENTRY_COUNT)
                return -1;
        acl_ea_header *header = (acl_ea_header*)acl_buf;
        header->a_version = (uint32_t)ACL_EA_VERSION;
        acl_ea_entry *entry = header->a_entries;

        entry->e_tag = ACL_USER_OBJ;
        entry->e_perm = (mode >> 6) & 7;

        ++entry;
        entry->e_tag = ACL_USER;
        entry->e_perm = (mode >> 6) & 7;
        entry->e_id = geteuid();

        ++entry;
        entry->e_tag = ACL_GROUP_OBJ;
        entry->e_perm = (mode >> 3) & 7;

        ++entry;
        entry->e_tag = ACL_GROUP;
        entry->e_perm = (mode >> 3) & 7;
        entry->e_id = getegid();

        ++entry;
        entry->e_tag = ACL_MASK;
        entry->e_perm = 0;

        ++entry;
        entry->e_tag = ACL_OTHER;
        entry->e_perm = mode & 7;

        return 0;
}
