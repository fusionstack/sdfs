#ifndef __LIST_H__
#define __LIST_H__

#include <stdint.h>

#ifndef NULL
#define NULL    0
#endif

#ifndef typeof
#define typeof __typeof__
#endif

/*
 * shamelessly stolean from linux-2.6/include/linux/list.h
 */

/*
 * These are non-NULL pointers that will result in page faults
 * under normal circumstances, used to verify that nobody uses
 * non-initialized list entries.
 */
#define LIST_POISON1  ((void *) 0x00100100)
#define LIST_POISON2  ((void *) 0x00200200)

/*
 * Simple doubly linked list implementation.
 *
 * Some of the internal functions ("__xxx") are useful when
 * manipulating whole lists rather than single entries, as
 * sometimes we already know the next/prev entries and we can
 * generate better code by using them directly rather than
 * using the generic single-entry routines.
 */

struct list_head {
        struct list_head *next, *prev;
};

#define LIST_HEAD_INIT(name) { &(name), &(name) }

#ifndef LIST_HEAD
#define LIST_HEAD(name) \
        struct list_head name = LIST_HEAD_INIT(name)
#endif

#define INIT_LIST_HEAD(ptr) do { \
        (ptr)->next = (ptr); (ptr)->prev = (ptr); \
} while (0)

/*
 * Insert a new entry between two known consecutive entries.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __list_add(struct list_head *newer,
                              struct list_head *prev,
                              struct list_head *next)
{
        next->prev = newer;
        newer->next = next;
        newer->prev = prev;
        prev->next = newer;
}

/**
 * list_add - add a new entry
 * @new: new entry to be added
 * @head: list head to add it after
 *
 * Insert a new entry after the specified head.
 * This is good for implementing stacks.
 */
static inline void list_add(struct list_head *newer, struct list_head *head)
{
        __list_add(newer, head, head->next);
}

/**
 * list_add_tail - add a new entry
 * @new: new entry to be added
 * @head: list head to add it before
 *
 * Insert a new entry before the specified head.
 * This is useful for implementing queues.
 */
static inline void list_add_tail(struct list_head *newer, struct list_head *head)
{
        __list_add(newer, head->prev, head);
}

/*
 * Delete a list entry by making the prev/next entries
 * point to each other.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __list_del(struct list_head * prev, struct list_head * next)
{
        next->prev = prev;
        prev->next = next;
}

/**
 * list_del - deletes entry from list.
 * @entry: the element to delete from the list.
 * Note: list_empty on entry does not return true after this, the entry is
 * in an undefined state.
 */
static inline void list_del(struct list_head *entry)
{
        __list_del(entry->prev, entry->next);
        entry->next = (struct list_head *)LIST_POISON1;
        entry->prev = (struct list_head *)LIST_POISON2;
}

/**
 * list_del_init - deletes entry from list and reinitialize it.
 * @entry: the element to delete from the list.
 */
static inline void list_del_init(struct list_head *entry)
{
        __list_del(entry->prev, entry->next);
        INIT_LIST_HEAD(entry);
}

/**
 * list_move - delete from one list and add as another's head
 * @list: the entry to move
 * @head: the head that will precede our entry
 */
static inline void list_move(struct list_head *list, struct list_head *head)
{
        __list_del(list->prev, list->next);
        list_add(list, head);
}

/**
 * list_move_tail - delete from one list and add as another's tail
 * @list: the entry to move
 * @head: the head that will follow our entry
 */
static inline void list_move_tail(struct list_head *list,
                                  struct list_head *head)
{
        __list_del(list->prev, list->next);
        list_add_tail(list, head);
}

/**
 * list_empty - tests whether a list is empty
 * @head: the list to test.
 */
static inline int list_empty(const struct list_head *head)
{
        return head->next == head;
}

/**
 * list_empty_careful - tests whether a list is
 * empty _and_ checks that no other CPU might be
 * in the process of still modifying either member
 *
 * NOTE: using list_empty_careful() without synchronization
 * can only be safe if the only activity that can happen
 * to the list entry is list_del_init(). Eg. it cannot be used
 * if another CPU could re-list_add() it.
 *
 * @head: the list to test.
 */
static inline int list_empty_careful(const struct list_head *head)
{
        struct list_head *next = head->next;
        return (next == head) && (next == head->prev);
}

#if 0
static inline void __list_splice(struct list_head *list,
                                 struct list_head *head)
{
        struct list_head *first = list->next;
        struct list_head *last = list->prev;
        struct list_head *at = head->next;

        first->prev = head;
        head->next = first;

        last->next = at;
        at->prev = last;
}

/**
 * list_splice - join two lists
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 */
static inline void list_splice(struct list_head *list, struct list_head *head)
{
        if (!list_empty(list))
                __list_splice(list, head);
}

/**
 * list_splice_init - join two lists and reinitialise the emptied list.
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 *
 * The list at @list is reinitialised
 */
static inline void list_splice_init(struct list_head *list,
                                    struct list_head *head)
{
        if (!list_empty(list)) {
                __list_splice(list, head);
                INIT_LIST_HEAD(list);
        }
}

#endif

static inline void __list_splice(const struct list_head *list,
				 struct list_head *prev,
				 struct list_head *next)
{
	struct list_head *first = list->next;
	struct list_head *last = list->prev;

	first->prev = prev;
	prev->next = first;

	last->next = next;
	next->prev = last;
}

/**
 * list_splice - join two lists, this is designed for stacks
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 */
static inline void list_splice(const struct list_head *list,
				struct list_head *head)
{
	if (!list_empty(list))
		__list_splice(list, head, head->next);
}

/**
 * list_splice_tail - join two lists, each list being a queue
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 */
static inline void list_splice_tail(struct list_head *list,
				struct list_head *head)
{
	if (!list_empty(list))
		__list_splice(list, head->prev, head);
}

/**
 * list_splice_init - join two lists and reinitialise the emptied list.
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 *
 * The list at @list is reinitialised
 */
static inline void list_splice_init(struct list_head *list,
				    struct list_head *head)
{
	if (!list_empty(list)) {
		__list_splice(list, head, head->next);
		INIT_LIST_HEAD(list);
	}
}

/**
 * list_splice_tail_init - join two lists and reinitialise the emptied list
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 *
 * Each of the lists is a queue.
 * The list at @list is reinitialised
 */
static inline void list_splice_tail_init(struct list_head *list,
					 struct list_head *head)
{
	if (!list_empty(list)) {
		__list_splice(list, head->prev, head);
		INIT_LIST_HEAD(list);
	}
}

#define __offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)

/* from linux-2.6/include/linux/kernel.h */
/**
 * __container_of - cast a member of a structure out to the containing structure
 * @ptr:        the pointer to the member.
 * @type:       the type of the container struct this is embedded in.
 * @member:     the name of the member within the struct.
 *
 */
#define __container_of(ptr, type, member) ({                      \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
        (type *)( (char *)__mptr - __offsetof(type,member) );})

/**
 * list_entry - get the struct for this entry
 * @ptr:        the &struct list_head pointer.
 * @type:       the type of the struct this is embedded in.
 * @member:     the name of the list_struct within the struct.
 */
#define list_entry(ptr, type, member) \
        __container_of(ptr, type, member)



#define List_GetFirst(head)                                             \
        ({                                                              \
         struct list_head *one = NULL;                                       \
         if ( !list_empty(head) ) {                                              \
         one = (head)->next;                                     \
         list_del(one);                                          \
         }                                                                                       \
         (one);                                                        \
         })

#define list_entry_first(head, type, member)    \
        list_entry(List_GetFirst(head), type, member)

/*
 * Alternative inline assembly with input.
 *
 * Pecularities:
 * No memory clobber here.
 * Argument numbers start with 1.
 * Best is to use constraints that are fixed size (like (%1) ... "r")
 * If you use variable sized constraints like "m" or "g" in the
 * replacement maake sure to pad to the worst case length.
 */
#ifndef __x86_64__
#define alternative_input(oldinstr, newinstr, feature, input...)               \
        asm volatile ("661:\n\t" oldinstr "\n662:\n"                           \
                      ".section .altinstructions,\"a\"\n"                      \
                      "  .align 4\n"                                           \
                      "  .long 661b\n"            /* label */                  \
                      "  .long 663f\n"	          /* new instruction */        \
                      "  .byte %c0\n"             /* feature bit */            \
                      "  .byte 662b-661b\n"       /* sourcelen */              \
                      "  .byte 664f-663f\n"       /* replacementlen */         \
                      ".previous\n"                                            \
                      ".section .altinstr_replacement,\"ax\"\n"                \
                      "663:\n\t" newinstr "\n664:\n"   /* replacement */       \
                      ".previous" :: "i" (feature), ##input)

#define X86_FEATURE_XMM	       (0*32+25) /* Streaming SIMD Extensions */

#define GENERIC_NOP4        ".byte 0x8d,0x74,0x26,0x00\n"

#define ASM_NOP4       GENERIC_NOP4

/* Prefetch instructions for Pentium III and AMD Athlon */
/* It's not worth to care about 3dnow! prefetches for the K6
   because they are microcoded there and very slow.
   However we don't do prefetches for pre XP Athlons currently
   That should be fixed. */

#define ARCH_HAS_PREFETCH
static inline void prefetch(const void *x)
{
	alternative_input(ASM_NOP4,
			  "prefetchnta (%1)",
			  X86_FEATURE_XMM,
			  "r" (x));
}

#else

static inline void prefetch(const void *x)
{
	(void) x;
}

#endif

/**
 * list_for_each	-	iterate over a list
 * @pos:        the &struct list_head to use as a loop counter.
 * @head:       the head for your list.
 */
#define list_for_each(pos, head) \
        for (pos = (head)->next; prefetch(pos->next), pos != (head); \
                pos = pos->next)

/**
 * __list_for_each	-	iterate over a list
 * @pos:        the &struct list_head to use as a loop counter.
 * @head:       the head for your list.
 *
 * This variant differs from list_for_each() in that it's the
 * simplest possible list iteration code, no prefetching is done.
 * Use this for code that knows the list to be very short (empty
 * or 1 entry) most of the time.
 */
#define __list_for_each(pos, head) \
        for (pos = (head)->next; pos != (head); pos = pos->next)

/**
 * list_for_each_prev  -        iterate over a list backwards
 * @pos:        the &struct list_head to use as a loop counter.
 * @head:       the head for your list.
 */
#define list_for_each_prev(pos, head) \
        for (pos = (head)->prev; prefetch(pos->prev), pos != (head); \
                pos = pos->prev)

/**
 * list_for_each_safe  -  iterate over a list safe against removal of list entry
 * @pos:        the &struct list_head to use as a loop counter.
 * @n:          another &struct list_head to use as temporary storage
 * @head:       the head for your list.
 */
#define list_for_each_safe(pos, n, head) \
        for (pos = (head)->next, n = pos->next; pos != (head); \
                pos = n, n = pos->next)

/**
 * list_for_each_prev_safe - iterate over a list backwards safe against removal of list entry
 * @pos:	the &struct list_head to use as a loop cursor.
 * @n:		another &struct list_head to use as temporary storage
 * @head:	the head for your list.
 */
#define list_for_each_prev_safe(pos, n, head) \
	for (pos = (head)->prev, n = pos->prev; \
	     prefetch(pos->prev), pos != (head); \
	     pos = n, n = pos->prev)

/**
 * list_for_each_entry -       iterate over list of given type
 * @pos:        the type * to use as a loop counter.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry(pos, head, member)                          \
        for (pos = list_entry((head)->next, typeof(*pos), member);      \
             prefetch(pos->member.next), &pos->member != (head);        \
             pos = list_entry(pos->member.next, typeof(*pos), member))

/**
 * list_for_each_entry_reverse - iterate backwards over list of given type.
 * @pos:        the type * to use as a loop counter.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry_reverse(pos, head, member)                  \
        for (pos = list_entry((head)->prev, typeof(*pos), member);      \
             prefetch(pos->member.prev), &pos->member != (head);        \
             pos = list_entry(pos->member.prev, typeof(*pos), member))

/**
 * list_prepare_entry - prepare a pos entry for use as a start point in
 *                      list_for_each_entry_continue
 * @pos:        the type * to use as a start point
 * @head:       the head of the list
 * @member:     the name of the list_struct within the struct.
 */
#define list_prepare_entry(pos, head, member) \
        ((pos) ? : list_entry(head, typeof(*pos), member))

/**
 * list_for_each_entry_continue -      iterate over list of given type
 *                              continuing after existing point
 * @pos:        the type * to use as a loop counter.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry_continue(pos, head, member)                 \
        for (pos = list_entry(pos->member.next, typeof(*pos), member);  \
             prefetch(pos->member.next), &pos->member != (head);       \
             pos = list_entry(pos->member.next, typeof(*pos), member))

/**
 * list_for_each_entry_safe - iterate over list of given type safe
 *                            against removal of list entry
 * @pos:        the type * to use as a loop counter.
 * @n:          another type * to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry_safe(pos, n, head, member)                   \
        for (pos = list_entry((head)->next, typeof(*pos), member),       \
        	n = list_entry(pos->member.next, typeof(*pos), member);	 \
             &pos->member != (head);                                     \
             pos = n, n = list_entry(n->member.next, typeof(*n), member))

/**
 * list_for_each_entry_safe_continue -	iterate over list of given type
 *            continuing after existing point safe against removal of list entry
 * @pos:        the type * to use as a loop counter.
 * @n:          another type * to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry_safe_continue(pos, n, head, member)                \
        for (pos = list_entry(pos->member.next, typeof(*pos), member),         \
                n = list_entry(pos->member.next, typeof(*pos), member);        \
             &pos->member != (head);                                           \
             pos = n, n = list_entry(n->member.next, typeof(*n), member)

/**
 * list_exists - entry is exists in list.
 * @entry: the element to compare from the list.
 * Note: list_empty on entry does not return true after this, the entry is
 * in an undefined state.
 */
static inline int list_exists(struct list_head *entry, struct list_head *list)
{
        struct list_head *pos, *n;

        list_for_each_safe(pos, n, list) {
                if (pos == entry) {
                        return 1;
                }
        }

        return 0;
}

/**
 * list_del_safe - deletes entry from list if entry exists.
 * @entry: the element to delete from the list.
 * Note: list_empty on entry does not return true after this, the entry is
 * in an undefined state.
 */
static inline int list_del_safe(struct list_head *entry, struct list_head *list)
{
        if (list_exists(entry, list)) {
                list_del(entry);
                return 1;
        }

        return 0;
}

/*
 * Double linked lists with a single pointer list head.
 * Mostly useful for hash tables where the two pointer list head is
 * too wasteful.
 * You lose the ability to access the tail in O(1).
 */

struct hlist_head {
        struct hlist_node *first;
};

struct hlist_node {
        struct hlist_node *next, **pprev;
};

#define HLIST_HEAD_INIT { .first = NULL }
#define HLIST_HEAD(name) struct hlist_head name = {  .first = NULL }
#define INIT_HLIST_HEAD(ptr) ((ptr)->first = NULL)
#define INIT_HLIST_NODE(ptr) ((ptr)->next = NULL, (ptr)->pprev = NULL)

static inline int hlist_unhashed(const struct hlist_node *h)
{
        return !h->pprev;
}

static inline int hlist_empty(const struct hlist_head *h)
{
        return !h->first;
}

static inline void __hlist_del(struct hlist_node *n)
{
        struct hlist_node *next = n->next;
        struct hlist_node **pprev = n->pprev;
        *pprev = next;
        if (next)
                next->pprev = pprev;
}

static inline void hlist_del(struct hlist_node *n)
{
        __hlist_del(n);
        n->next = (struct hlist_node *)LIST_POISON1;
        n->pprev = (struct hlist_node **)LIST_POISON2;
}

static inline void hlist_del_init(struct hlist_node *n)
{
        if (n->pprev)  {
                __hlist_del(n);
                INIT_HLIST_NODE(n);
        }
}

static inline void hlist_add_head(struct hlist_node *n, struct hlist_head *h)
{
        struct hlist_node *first = h->first;
        n->next = first;
        if (first)
                first->pprev = &n->next;
        h->first = n;
        n->pprev = &h->first;
}

/* next must be != NULL */
static inline void hlist_add_before(struct hlist_node *n,
                                    struct hlist_node *next)
{
        n->pprev = next->pprev;
        n->next = next;
        next->pprev = &n->next;
        *(n->pprev) = n;
}

static inline void hlist_add_after(struct hlist_node *n,
                                   struct hlist_node *next)
{
        next->next = n->next;
        n->next = next;
        next->pprev = &n->next;

        if(next->next)
                next->next->pprev  = &next->next;
}

#define hlist_entry(ptr, type, member) __container_of(ptr,type,member)

#define hlist_for_each(pos, head) \
        for (pos = (head)->first; pos && ({ prefetch(pos->next); 1; }); \
             pos = pos->next)

#define hlist_for_each_safe(pos, n, head) \
        for (pos = (head)->first; pos && ({ n = pos->next; 1; }); \
             pos = n)

/**
 * hlist_for_each_entry	- iterate over list of given type
 * @tpos:       the type * to use as a loop counter.
 * @pos:        the &struct hlist_node to use as a loop counter.
 * @head:       the head for your list.
 * @member:     the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry(tpos, pos, head, member)                    \
        for (pos = (head)->first;                                        \
             pos && ({ prefetch(pos->next); 1;})                         \
             && ({ tpos = hlist_entry(pos, typeof(*tpos), member); 1;}); \
             pos = pos->next)

/**
 * hlist_for_each_entry_continue - iterate over a hlist continuing
 *                                 after existing point
 * @tpos:       the type * to use as a loop counter.
 * @pos:        the &struct hlist_node to use as a loop counter.
 * @member:     the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry_continue(tpos, pos, member)                 \
        for (pos = (pos)->next;                                          \
             pos && ({ prefetch(pos->next); 1;})                         \
             && ({ tpos = hlist_entry(pos, typeof(*tpos), member); 1;}); \
             pos = pos->next)

/**
 * hlist_for_each_entry_from - iterate over a hlist continuing from existing point
 * @tpos:       the type * to use as a loop counter.
 * @pos:        the &struct hlist_node to use as a loop counter.
 * @member:     the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry_from(tpos, pos, member)                       \
        for (; pos && ({ prefetch(pos->next); 1;})                         \
               && ({ tpos = hlist_entry(pos, typeof(*tpos), member); 1;}); \
             pos = pos->next)

/**
 * hlist_for_each_entry_safe - iterate over list of given type safe
 *                             against removal of list entry
 * @tpos:       the type * to use as a loop counter.
 * @pos:        the &struct hlist_node to use as a loop counter.
 * @n:          another &struct hlist_node to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry_safe(tpos, pos, n, head, member)            \
        for (pos = (head)->first;                                        \
             pos && ({ n = pos->next; 1; })                              \
             && ({ tpos = hlist_entry(pos, typeof(*tpos), member); 1;}); \
             pos = n)

static inline int list_size(struct list_head *head) {
        int size = 0;
        struct list_head *pos;

        list_for_each(pos, head) {
                size += 1;
        }

        return size;
}

// -- list with count

typedef struct {
        struct list_head list;
        uint32_t count;
} count_list_t;

static inline void count_list_init(count_list_t *head) {
        INIT_LIST_HEAD(&head->list);
        head->count = 0;
}

static inline void count_list_add_tail(struct list_head *newer, count_list_t *head) {
        list_add_tail(newer, &head->list);
        head->count++;
}

static inline void count_list_add(struct list_head *newer, count_list_t *head) {
        list_add(newer, &head->list);
        head->count++;
}

static inline void count_list_del(struct list_head *pos, count_list_t *head) {
        list_del(pos);
        head->count--;
}

static inline void count_list_del_init(struct list_head *pos, count_list_t *head) {
        list_del_init(pos);
        head->count--;
}

static inline void count_list_free(count_list_t *head, int (*free_fn)(void **)) {
        struct list_head *pos, *n;

        list_for_each_safe(pos, n, &head->list) {
                count_list_del_init(pos, head);
                if (free_fn) {
                        free_fn((void **)&pos);
                }
        }

        // assert(head->count == 0);
}


#endif
