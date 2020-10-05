/*
 * fatcache - memcache on ssd.
 * Copyright (C) 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _FC_SLAB_H_
#define _FC_SLAB_H_

struct slab {
    uint32_t  magic;     /* slab magic (const) */
    uint32_t  sid;       /* slab id */
    uint8_t   cid;       /* slab class id */
    uint8_t   unused[3]; /* unused */
    uint8_t   data[1];   /* opaque data */
};

#define SLAB_MAGIC      0xdeadbeef
#define SLAB_HDR_SIZE   offsetof(struct slab, data)
#define SLAB_MIN_SIZE   ((size_t) MB)
#define SLAB_SIZE       MB
#define SLAB_MAX_SIZE   ((size_t) (512 * MB))
#define MAX_HOLE_LENGTH      13107     /* 13107 == 1048576 / 80 */

typedef struct Hole_item{
    uint16_t hole_index;
    struct Hole_item * next;
} hole_item;

typedef struct Lru_head{
    struct slabinfo * head;
    struct slabinfo * tail;
} lru_head;

struct slabinfo {
    uint32_t              sid;    /* slab id (const) */
    uint32_t              addr;   /* address as slab_size offset from memory / disk base */
    TAILQ_ENTRY(slabinfo) tqe;    /* link in free q / partial q / full q */
    uint32_t              nalloc; /* # item allocated (monotonic) */
    //nfree is unused! just removed it to save mem
    // uint32_t              nfree;  /* # item freed (monotonic) */ //you can get it from c->nitem == sinfo->nalloc.
    uint8_t               cid;    /* class id */
    unsigned              mem:1;  /* memory? */
    hole_item*            hole_head;/* hole item queue head node */
    /* below is for simple double-linked LRU */
    struct slabinfo * pre;
    struct slabinfo * next;
};

TAILQ_HEAD(slabhinfo, slabinfo);

struct slabclass {
    uint32_t         nitem;           /* # item per slab (const), how many items this slab class can store */
    size_t           size;            /* item size (const) */
    size_t           slack;           /* unusable slack space (const) */
    struct slabhinfo partial_msinfoq; /* partial slabinfo q */
    uint32_t         nmslab;          /* # memory slab */
    uint32_t         ndslab;          /* # disk slab */
    uint64_t         nevict;          /* # eviect time */
    uint64_t         nused_item;      /* # used item */
    // struct slab * hot_slab;           /* this pointer aims a dynamic hot slab, this hot slab stores updated items */
    struct slabinfo * hot_slabinfo;   /* this slab info is for hot slab, which has the same level as partial slab */
};

#define SLABCLASS_MIN_ID        0
#define SLABCLASS_MAX_ID        (UCHAR_MAX - 1)
#define SLABCLASS_INVALID_ID    UCHAR_MAX
#define SLABCLASS_MAX_IDS       UCHAR_MAX

struct slabinfo* sid_to_sinfo(uint32_t sid);
size_t cid_to_size(uint8_t cid);
uint64_t slab_nflush(void);

bool slab_valid_id(uint8_t cid);
size_t slab_data_size(void);
void slab_print(void);
uint8_t slab_cid(size_t size);

struct item *slab_get_item(uint8_t cid, bool update);

void slab_put_item(struct item *it);
struct item *slab_read_item(uint32_t sid, uint32_t addr);

rstatus_t slab_init(void);
void slab_deinit(void);

uint32_t slab_msinfo_nalloc(void);
uint32_t slab_msinfo_nfree(void);
uint32_t slab_msinfo_nfull(void);
uint32_t slab_msinfo_npartial(void);
uint32_t slab_dsinfo_nalloc(void);
uint32_t slab_dsinfo_nfree(void);
uint32_t slab_dsinfo_nfull(void);

void lru_set(lru_head* lru, struct slabinfo* item);
void lru_remove_head(lru_head* lruh);


uint64_t slab_nevict(void);

uint8_t slab_max_cid(void);
uint8_t slab_get_cid(uint32_t sd);
struct slabclass *slab_get_class_by_cid(uint8_t cid);
bool slab_incr_chunks_by_sid(uint32_t sid, int n);

#endif
