#ifndef _BPLUS_TREE_H
#define _BPLUS_TREE_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <iostream>

namespace bplustree{

struct DataItem{
    size_t key;
    int32_t page;
    int32_t pos;
};

/* 5 node caches are needed at least for self, left and right sibling, sibling
 * of sibling, parent and node seeking */
#define MIN_CACHE_NUM 5

#define list_entry(ptr, type, member) \
        ((type *)((char *)(ptr) - (size_t)(&((type *)0)->member)))

#define list_first_entry(ptr, type, member) \
	list_entry((ptr)->next, type, member)

#define list_last_entry(ptr, type, member) \
	list_entry((ptr)->prev, type, member)

#define list_for_each(pos, head) \
        for (pos = (head)->next; pos != (head); pos = pos->next)

#define list_for_each_safe(pos, n, head) \
        for (pos = (head)->next, n = pos->next; pos != (head); \
                pos = n, n = pos->next)

typedef size_t my_key_t;

struct list_head {
        struct list_head *prev, *next;
};

static inline void list_init(struct list_head *link)
{
        link->prev = link;
        link->next = link;
}

static inline void
__list_add(struct list_head *link, struct list_head *prev, struct list_head *next)
{
        link->next = next;
        link->prev = prev;
        next->prev = link;
        prev->next = link;
}

static inline void __list_del(struct list_head *prev, struct list_head *next)
{
        prev->next = next;
        next->prev = prev;
}

static inline void list_add(struct list_head *link, struct list_head *prev)
{
        __list_add(link, prev, prev->next);
}

static inline void list_add_tail(struct list_head *link, struct list_head *head)
{
	__list_add(link, head->prev, head);
}

static inline void list_del(struct list_head *link)
{
        __list_del(link->prev, link->next);
        list_init(link);
}

static inline int list_empty(const struct list_head *head)
{
	return head->next == head;
}

typedef struct bplus_node {
        off_t self;
        off_t parent;
        off_t prev;
        off_t next;
        int type;
        /* If leaf node, it specifies  count of entries,
         * if non-leaf node, it specifies count of children(branches) */
        int children;
} bplus_node;

/*
struct bplus_non_leaf {
        off_t self;
        off_t parent;
        off_t prev;
        off_t next;
        int type;
        int children;
        my_key_t key[BPLUS_MAX_ORDER - 1];
        off_t sub_ptr[BPLUS_MAX_ORDER];
};

struct bplus_leaf {
        off_t self;
        off_t parent;
        off_t prev;
        off_t next;
        int type;
        int entries;
        my_key_t key[BPLUS_MAX_ENTRIES];
        size_tdata[BPLUS_MAX_ENTRIES];
};
*/

typedef struct free_block {
        struct list_head link;
        off_t offset;
} free_block;

struct bplus_tree {
        char *caches;
        int used[MIN_CACHE_NUM];
        char filename[1024];
        int fd;
        int level;
        off_t root;
        off_t file_size;
        struct list_head free_blocks;
};

void bplus_tree_dump(struct bplus_tree *tree);
size_t bplus_tree_get(struct bplus_tree *tree, my_key_t key, bool &flag);
int bplus_tree_put(struct bplus_tree *tree, my_key_t key, size_t data, bool flag);
size_t bplus_tree_get_range(struct bplus_tree *tree, my_key_t key1, my_key_t key2, int fd);
struct bplus_tree *bplus_tree_init(char *filename, int block_size);
void bplus_tree_deinit(struct bplus_tree *tree);
int bplus_open(char *filename);
void bplus_close(int fd);
size_t bplus_tree_update(struct bplus_tree *tree, my_key_t key, size_t data);

}

#endif  /* _BPLUS_TREE_H */
