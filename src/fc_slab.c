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

#include <fc_core.h>
#include <stdlib.h>
#include <stdio.h>

#define USE_LRU 1

extern struct settings settings;

static uint32_t nfree_msinfoq; /* # free memory slabinfo q */
static struct slabhinfo free_msinfoq; /* free memory slabinfo q */
static uint32_t nfull_msinfoq; /* # full memory slabinfo q */
static struct slabhinfo full_msinfoq; /* # full memory slabinfo q */

static uint32_t nfree_dsinfoq; /* # free disk slabinfo q */
static struct slabhinfo free_dsinfoq; /* free disk slabinfo q */
static uint32_t nfull_dsinfoq; /* # full disk slabinfo q */
static struct slabhinfo full_dsinfoq; /* full disk slabinfo q */
lru_head* lruh;
lru_head* lruh_disk;

static uint8_t nctable; /* # class table entry */
static struct slabclass* ctable; /* table of slabclass indexed by cid */

static uint32_t nstable; /* # slab table entry */
static struct slabinfo* stable; /* table of slabinfo indexed by sid */

static uint8_t* mstart; /* memory slab start */
static uint8_t* mend; /* memory slab end */

static off_t dstart; /* disk start */
static off_t dend; /* disk end */
static int fd; /* disk file descriptor */

static size_t mspace; /* memory space */
static size_t dspace; /* disk space */
static uint32_t nmslab; /* # memory slabs */
static uint32_t ndslab; /* # disk slabs */

static uint64_t nevict;
static uint64_t nflush;
static uint8_t* evictbuf; /* evict buffer */
static uint8_t* readbuf; /* read buffer */

/* for itemx to call, itemx can't access stable */
struct slabinfo*
sid_to_sinfo(uint32_t sid)
{
    return &stable[sid];
}

/* for itemx to call, itemx can't access ctable */
size_t
cid_to_size(uint8_t cid)
{
    return (&ctable[cid])->size;
}

/*
 * Return the maximum space available for item sized chunks in a given
 * slab. Slab cannot contain more than 2^32 bytes (4G).
 */
size_t
slab_data_size(void)
{
    return settings.slab_size - SLAB_HDR_SIZE; //default 1M - ?
}

/*
 * Return true if slab class id cid is valid and within bounds, otherwise
 * return false.
 */
bool slab_valid_id(uint8_t cid)
{
    if (cid >= SLABCLASS_MIN_ID && cid <= settings.profile_last_id) {
        return true;
    }

    return false;
}

void slab_print(void)
{
    uint8_t cid; /* slab class id */
    struct slabclass* c; /* slab class */

    loga("slab size %zu, slab hdr size %zu, item hdr size %zu, "
         "item chunk size %zu",
        settings.slab_size, SLAB_HDR_SIZE,
        ITEM_HDR_SIZE, settings.chunk_size);

    loga("index memory %zu, slab memory %zu, disk space %zu",
        0, mspace, dspace);

    for (cid = SLABCLASS_MIN_ID; cid < nctable; cid++) {
        c = &ctable[cid];
        loga("class %3" PRId8 ": items %7" PRIu32 "  size %7zu  data %7zu  "
             "slack %7zu",
            cid, c->nitem, c->size, c->size - ITEM_HDR_SIZE,
            c->slack);
    }
}

/*
 * Return the cid of the slab which can store an item of a given size.
 *
 * Return SLABCLASS_INVALID_ID, for large items which cannot be stored in
 * any of the configured slabs.
 */
uint8_t
slab_cid(size_t size)
{
    uint8_t cid, imin, imax;

    ASSERT(size != 0);

    /* binary search */
    imin = SLABCLASS_MIN_ID;
    imax = nctable;
    while (imax >= imin) {
        cid = (imin + imax) / 2;
        if (size > ctable[cid].size) {
            imin = cid + 1;
        } else if (cid > SLABCLASS_MIN_ID && size <= ctable[cid - 1].size) {
            imax = cid - 1;
        } else {
            break;
        }
    }

    if (imin > imax) {
        /* size too big for any slab */
        return SLABCLASS_INVALID_ID;
    }

    return cid;
}

/*
 * Return true if all items in the slab have been allocated, else
 * return false.
 */
static bool
slab_full(struct slabinfo* sinfo)
{
    struct slabclass* c;

    ASSERT(sinfo->cid >= SLABCLASS_MIN_ID && sinfo->cid < nctable);
    c = &ctable[sinfo->cid];

    return (c->nitem == sinfo->nalloc) ? true : false;
}

/*
 * Return and optionally verify the memory slab with the given slab_size
 * offset from base mstart.
 */
static void*
slab_from_maddr(uint32_t addr, bool verify)
{
    struct slab* slab;
    off_t off;

    off = (off_t)addr * settings.slab_size;
    slab = (struct slab*)(mstart + off);
    if (verify) {
        ASSERT(mstart + off < mend);
        ASSERT(slab->magic == SLAB_MAGIC);
        ASSERT(slab->sid < nstable);
        ASSERT(stable[slab->sid].sid == slab->sid);
        ASSERT(stable[slab->sid].cid == slab->cid);
        ASSERT(stable[slab->sid].mem == 1);
    }

    return slab;
}

/*
 * Return the slab_size offset for the given disk slab from the base
 * of the disk.
 */
static off_t
slab_to_daddr(struct slabinfo* sinfo)
{
    off_t off;

    ASSERT(!sinfo->mem);

    off = dstart + ((off_t)sinfo->addr * settings.slab_size);
    ASSERT(off < dend);

    return off;
}

/*
 * Return and optionally verify the idx^th item with a given size in the
 * in given slab.
 * Note: idx == sinfo->nalloc, size is for this class level
 * 
 */
static struct item*
slab_to_item(struct slab* slab, uint32_t idx, size_t size, bool verify)
{
    struct item* it;

    ASSERT(slab->magic == SLAB_MAGIC);
    ASSERT(idx <= stable[slab->sid].nalloc);
    ASSERT(idx * size < settings.slab_size);

    it = (struct item*)((uint8_t*)slab->data + (idx * size));

    if (verify) {
        ASSERT(it->magic == ITEM_MAGIC);
        ASSERT(it->cid == slab->cid);
        ASSERT(it->sid == slab->sid);
    }

    return it;
}

//When there is no free indexs or slabs, evict data out of system
static rstatus_t
slab_evict(void)
{   
    log_debug(LOG_DEBUG, "evict slab");
    struct slabclass* c; /* slab class */
    struct slabinfo* sinfo; /* disk slabinfo */
    struct slab* slab; /* read slab */
    size_t size; /* bytes to read */
    off_t off; /* offset */
    int n; /* read bytes */
    uint32_t idx; /* idx^th item */

    ASSERT(!TAILQ_EMPTY(&full_dsinfoq));
    ASSERT(nfull_dsinfoq > 0);

    // sinfo = TAILQ_FIRST(&full_dsinfoq); //old version
    if (lruh_disk->head && USE_LRU) {
        sinfo = lruh_disk->head;
        lru_remove_head(lruh_disk);
    }else{
        sinfo = TAILQ_FIRST(&full_dsinfoq);
    }
    
    nfull_dsinfoq--;
    TAILQ_REMOVE(&full_dsinfoq, sinfo, tqe);
    ASSERT(!sinfo->mem);
    ASSERT(sinfo->addr < ndslab);

    /* read the slab */
    slab = (struct slab*)evictbuf; //already inited
    size = settings.slab_size;
    off = slab_to_daddr(sinfo);
    n = pread(fd, slab, size, off);
    if (n < size) {
        log_error("pread fd %d %zu bytes at offset %" PRIu64 " failed: %s", fd,
            size, (uint64_t)off, strerror(errno));
        return FC_ERROR;
    }
    ASSERT(slab->magic == SLAB_MAGIC);
    ASSERT(slab->sid == sinfo->sid);
    ASSERT(slab->cid == sinfo->cid);
    ASSERT(slab_full(sinfo));

    /* evict all items from the slab */
    for (c = &ctable[slab->cid], idx = 0; idx < c->nitem; idx++) {
        struct item* it = slab_to_item(slab, idx, c->size, true);
        if (itemx_getx(it->hash, it->md) != NULL) {
            itemx_removex(it->hash, it->md);
        }
    }

    log_debug(LOG_DEBUG, "evict slab at disk (sid %" PRIu32 ", addr %" PRIu32 ")",
        sinfo->sid, sinfo->addr);

    /* move disk slab from full to free q */
    nfree_dsinfoq++;
    TAILQ_INSERT_TAIL(&free_dsinfoq, sinfo, tqe);
    nevict++;
    c->nevict++;
    c->ndslab--;

    return FC_OK;
}

static void
slab_swap_addr(struct slabinfo* msinfo, struct slabinfo* dsinfo)
{
    uint32_t m_addr;

    ASSERT(msinfo->mem);
    ASSERT(!dsinfo->mem);

    /* on address swap, sid and cid are left untouched */
    m_addr = msinfo->addr;

    msinfo->addr = dsinfo->addr;
    msinfo->mem = 0;

    dsinfo->addr = m_addr;
    dsinfo->mem = 1;
}

//flush to disk
static rstatus_t
_slab_drain(void)
{   
    struct slabinfo *msinfo, *dsinfo; /* memory and disk slabinfo */
    struct slab* slab; /* slab to write */
    size_t size; /* bytes to write */
    off_t off; /* offset to write at */
    int n; /* written bytes */

    ASSERT(!TAILQ_EMPTY(&full_msinfoq));
    ASSERT(nfull_msinfoq > 0);

    ASSERT(!TAILQ_EMPTY(&free_dsinfoq));
    ASSERT(nfree_dsinfoq > 0);

    /* get memory sinfo from full q */

    // flush the first item in full queue (FIFO)
    // slab is kindof big granularity, so just use a simple implements instead of clock algorithm...
    // msinfo = TAILQ_FIRST(&full_msinfoq); // old version FIFO
    if (lruh->head && USE_LRU) {
        msinfo = lruh->head;
        lru_remove_head(lruh);
    }else{
        msinfo = TAILQ_FIRST(&full_msinfoq);
    }
    
    
    nfull_msinfoq--;
    TAILQ_REMOVE(&full_msinfoq, msinfo, tqe);

    ASSERT(msinfo->mem);
    ASSERT(slab_full(msinfo));

    /* get disk sinfo from free q */
    dsinfo = TAILQ_FIRST(&free_dsinfoq);
    nfree_dsinfoq--;
    TAILQ_REMOVE(&free_dsinfoq, dsinfo, tqe);
    ASSERT(!dsinfo->mem);

    /* drain the memory to disk slab */
    slab = slab_from_maddr(msinfo->addr, true);
    size = settings.slab_size;
    off = slab_to_daddr(dsinfo);
    n = pwrite(fd, slab, size, off);
    if (n < size) {
        log_error("pwrite fd %d %zu bytes at offset %" PRId64 " failed: %s",
            fd, size, off, strerror(errno));
        return FC_ERROR;
    }

    ctable[msinfo->cid].nmslab--;
    ctable[msinfo->cid].ndslab++;
    log_debug(LOG_DEBUG, "drain slab at memory (sid %" PRIu32 " addr %" PRIu32 ") "
                         "to disk (sid %" PRIu32 " addr %" PRIu32 ")",
        msinfo->sid,
        msinfo->addr, dsinfo->sid, dsinfo->addr);

    /* swap msinfo <> dsinfo addresses */
    slab_swap_addr(msinfo, dsinfo);

    /* move dsinfo (now a memory sinfo) to free q */
    nfree_msinfoq++;
    TAILQ_INSERT_TAIL(&free_msinfoq, dsinfo, tqe);

    /* move msinfo (now a disk sinfo) to full q */
    nfull_dsinfoq++;
    TAILQ_INSERT_TAIL(&full_dsinfoq, msinfo, tqe);
    nflush++;
    return FC_OK;
}

static rstatus_t
slab_drain(void)
{
    rstatus_t status;

    // still have some free slabs in SSD
    if (!TAILQ_EMPTY(&free_dsinfoq)) {
        ASSERT(nfree_dsinfoq > 0);
        return _slab_drain();
    }

    // no free slabs in SSD, flush + evict
    status = slab_evict();
    if (status != FC_OK) {
        return status;
    }

    ASSERT(!TAILQ_EMPTY(&free_dsinfoq));
    ASSERT(nfree_dsinfoq > 0);

    return _slab_drain();
}

// get item space from the first partial slab
static struct item*
_slab_get_item(uint8_t cid, bool update)
{
    struct slabclass* c; 
    struct slabinfo* sinfo;
    struct slab* slab;
    struct item* it;

    ASSERT(cid >= SLABCLASS_MIN_ID && cid < nctable);
    c = &ctable[cid]; // ctable is a global table. get slab class by cid

    // get slab info
    if (update == true) {
        sinfo = c->hot_slabinfo;
        log_debug(LOG_VERB, "use hot slab");
        ASSERT(sinfo != NULL);
        ASSERT(!slab_full(sinfo));
    } else {
        /* allocate new item from partial slab */
        ASSERT(!TAILQ_EMPTY(&c->partial_msinfoq));
        sinfo = TAILQ_FIRST(&c->partial_msinfoq);
        ASSERT(!slab_full(sinfo));
    }

    slab = slab_from_maddr(sinfo->addr, true); //拿到slab！

    /* consume an new item from partial slab's end area */
    // use deleted area
    if (sinfo->hole_head) {
        uint16_t hole_index = sinfo->hole_head->hole_index;
        hole_item* tmp = sinfo->hole_head;
        sinfo->hole_head = sinfo->hole_head->next;
        // use this empty space to create a item
        it = (struct item*)((uint8_t*)slab->data + (hole_index * c->size));
        it->offset = (uint32_t)((uint8_t*)it - (uint8_t*)slab);
        log_debug(LOG_VERB, "use deleted area");
        free(tmp);
    } else {
        it = slab_to_item(slab, sinfo->nalloc, c->size, false);
        it->offset = (uint32_t)((uint8_t*)it - (uint8_t*)slab);
    }

    it->sid = slab->sid;
    sinfo->nalloc++;

    // deal with full partial slab situations
    if (slab_full(sinfo)) {
        if (update == true) {
            c->hot_slabinfo = NULL;//mark as NULL
        } else {
            /* move memory slab from partial to full q */
            TAILQ_REMOVE(&c->partial_msinfoq, sinfo, tqe);
        }
        nfull_msinfoq++;
        TAILQ_INSERT_TAIL(&full_msinfoq, sinfo, tqe);
    }

    log_debug(LOG_VERB, "get it at offset %" PRIu32 " with cid %" PRIu8 "",
        it->offset, it->cid);

    // for LRU (it's write-sensitve, because flash storage can also provide ideal read performance)
    // two LRU double-linked list 
    if (slab_full(sinfo) && USE_LRU) {
        if (sinfo->mem) {
            lru_set(lruh, sinfo);
        } else {
            lru_set(lruh_disk, sinfo);
        }
    }

    return it;
}

// get a proper slab in this slab class, and then get a space from slab to store this item
// "update" param would decide to store on hot slab or not
// this func is only for make sure there is a usable slab (partial slab or hot slab)
struct item*
slab_get_item(uint8_t cid, bool update)
{
    rstatus_t status;
    struct slabclass* c;
    struct slabinfo* sinfo;
    struct slab* slab;

    ASSERT(cid >= SLABCLASS_MIN_ID && cid < nctable);
    c = &ctable[cid];

    //index memory space is full
    if (itemx_empty()) {
        status = slab_evict();
        if (status != FC_OK) {
            return NULL;
        }
    }

    //hot data would return item address in hot slab
    if (update == true) {
        if (c->hot_slabinfo == NULL) { // this class has no hot slab
            if (!TAILQ_EMPTY(&free_msinfoq)) { // any free slab?
                /* move memory slab from free to partial q */
                sinfo = TAILQ_FIRST(&free_msinfoq);

                ASSERT(nfree_msinfoq > 0);
                nfree_msinfoq--;
                c->nmslab++;
                TAILQ_REMOVE(&free_msinfoq, sinfo, tqe);

                /* init hot slab */
                sinfo->nalloc = 0;
                sinfo->cid = cid;
                c->hot_slabinfo = sinfo;                
                slab = slab_from_maddr(sinfo->addr, false);
                slab->magic = SLAB_MAGIC;
                slab->cid = cid;
                /* unused[] is left uninitialized */
                slab->sid = sinfo->sid;
                /* data[] is initialized on-demand */

                return _slab_get_item(cid, update); // get item space from hot slab
            } else {
                status = slab_drain(); // move a slab *to disk
                if (status != FC_OK) {
                    return NULL;
                } else {
                    return slab_get_item(cid, update); // successfully moved, and retry this process
                }
            }
        } else {
            // Note: the hot slab here must be partial
            return _slab_get_item(cid, update); // get item space from hot slab

        }
    }

    if (!TAILQ_EMPTY(&c->partial_msinfoq)) {
        return _slab_get_item(cid, update);
    }
    
    if (!TAILQ_EMPTY(&free_msinfoq)) {
        // no partial slabs, use free
        /* move memory slab from free to partial q */
        sinfo = TAILQ_FIRST(&free_msinfoq);

        ASSERT(nfree_msinfoq > 0);
        nfree_msinfoq--;
        c->nmslab++;
        TAILQ_REMOVE(&free_msinfoq, sinfo, tqe);

        /* init partial sinfo */
        TAILQ_INSERT_HEAD(&c->partial_msinfoq, sinfo, tqe);
        /* sid is already initialized by slab_init */
        /* addr is already initialized by slab_init */
        sinfo->nalloc = 0;
        // sinfo->nfree = 0;
        sinfo->cid = cid;
        /* mem is already initialized by slab_init */
        ASSERT(sinfo->mem == 1);

        /* init slab of partial sinfo */
        slab = slab_from_maddr(sinfo->addr, false);
        slab->magic = SLAB_MAGIC;
        slab->cid = cid;
        /* unused[] is left uninitialized */
        slab->sid = sinfo->sid;
        /* data[] is initialized on-demand */

        return _slab_get_item(cid, update);
    }

    ASSERT(!TAILQ_EMPTY(&full_msinfoq));
    ASSERT(nfull_msinfoq > 0);

    // memory slabs are all full
    status = slab_drain();
    if (status != FC_OK) {
        return NULL;
    }

    return slab_get_item(cid, update);
}

void slab_put_item(struct item* it)
{
    log_debug(LOG_INFO, "put it '%.*s' at offset %" PRIu32 " with cid %" PRIu8,
        it->nkey, item_key(it), it->offset, it->cid);
}

struct item*
slab_read_item(uint32_t sid, uint32_t addr)
{
    struct slabclass* c; /* slab class */
    struct item* it; /* item */
    struct slabinfo* sinfo; /* slab info */
    int n; /* bytes read */
    off_t off; /* offset to read from */
    off_t aligned_off; /* aligned offset to read from */
    size_t aligned_size; /* aligned size to read */

    ASSERT(sid < nstable);
    ASSERT(addr < settings.slab_size);

    sinfo = &stable[sid];
    c = &ctable[sinfo->cid];
    it = NULL;

    if (sinfo->mem) {
        off = (off_t)sinfo->addr * settings.slab_size + addr;
        fc_memcpy(readbuf, mstart + off, c->size);
        it = (struct item*)readbuf;
        goto done;
    }

    off = slab_to_daddr(sinfo) + addr;
    aligned_off = ROUND_DOWN(off, 512);
    aligned_size = ROUND_UP((c->size + (off - aligned_off)), 512);

    n = pread(fd, readbuf, aligned_size, aligned_off);
    if (n < aligned_size) {
        log_error("pread fd %d %zu bytes at offset %" PRIu64 " failed: %s", fd,
            aligned_size, (uint64_t)aligned_off, strerror(errno));
        return NULL;
    }
    it = (struct item*)(readbuf + (off - aligned_off));

done:
    ASSERT(it->magic == ITEM_MAGIC);
    ASSERT(it->cid == sinfo->cid);
    ASSERT(it->sid == sinfo->sid);

    return it;
}

static rstatus_t
slab_init_ctable(void)
{
    struct slabclass* c;
    uint8_t cid;
    size_t* profile;

    ASSERT(settings.profile_last_id <= SLABCLASS_MAX_ID);

    profile = settings.profile;
    nctable = settings.profile_last_id + 1;
    ctable = fc_alloc(sizeof(*ctable) * nctable);
    if (ctable == NULL) {
        return FC_ENOMEM;
    }

    for (cid = SLABCLASS_MIN_ID; cid < nctable; cid++) {
        c = &ctable[cid];
        c->nitem = slab_data_size() / profile[cid];
        c->size = profile[cid];
        c->slack = slab_data_size() - (c->nitem * c->size);
        TAILQ_INIT(&c->partial_msinfoq);
        c->nmslab = 0;
        c->ndslab = 0;
        c->nevict = 0;
        c->nused_item = 0;
        c->hot_slabinfo = NULL;
    }

    return FC_OK;
}

static void
slab_deinit_ctable(void)
{
}

static rstatus_t
slab_init_stable(void)
{
    struct slabinfo* sinfo;
    uint32_t i, j;

    nstable = nmslab + ndslab;
    stable = fc_alloc(sizeof(*stable) * nstable);
    if (stable == NULL) {
        return FC_ENOMEM;
    }

    /* init memory slabinfo q  */
    for (i = 0; i < nmslab; i++) {
        sinfo = &stable[i];

        sinfo->sid = i;
        sinfo->addr = i;
        sinfo->nalloc = 0;
        // sinfo->nfree = 0;
        sinfo->cid = SLABCLASS_INVALID_ID;
        sinfo->mem = 1;
        //init hole system
        sinfo->hole_head = NULL;
        nfree_msinfoq++;
        TAILQ_INSERT_TAIL(&free_msinfoq, sinfo, tqe);
    }

    /* init disk slabinfo q */
    for (j = 0; j < ndslab && i < nstable; i++, j++) {
        sinfo = &stable[i];

        sinfo->sid = i;
        sinfo->addr = j;
        sinfo->nalloc = 0;
        // sinfo->nfree = 0;
        sinfo->cid = SLABCLASS_INVALID_ID;
        sinfo->mem = 0;
        sinfo->hole_head = NULL;
        nfree_dsinfoq++;
        TAILQ_INSERT_TAIL(&free_dsinfoq, sinfo, tqe);
    }
    sinfo->next = NULL;
    sinfo->pre = NULL;
    return FC_OK;
}

static void
slab_deinit_stable(void)
{
}

rstatus_t
slab_init(void)
{
    rstatus_t status;
    size_t size;
    uint32_t ndchunk;

    nfree_msinfoq = 0;
    TAILQ_INIT(&free_msinfoq);
    nfull_msinfoq = 0;
    TAILQ_INIT(&full_msinfoq);

    nfree_dsinfoq = 0;
    TAILQ_INIT(&free_dsinfoq);
    nfull_dsinfoq = 0;
    TAILQ_INIT(&full_dsinfoq);

    nctable = 0;
    ctable = NULL;

    nstable = 0;
    stable = NULL;

    mstart = NULL;
    mend = NULL;

    dstart = 0;
    dend = 0;
    fd = -1;

    mspace = 0;
    dspace = 0;
    nmslab = 0;
    ndslab = 0;

    evictbuf = NULL;
    readbuf = NULL;

    if (settings.ssd_device == NULL) {
        log_error("ssd device file must be specified");
        return FC_ERROR;
    }

    /* init slab class table */
    status = slab_init_ctable();
    if (status != FC_OK) {
        return status;
    }

    /* init nmslab, mstart and mend */
    nmslab = MAX(nctable, settings.max_slab_memory / settings.slab_size);
    mspace = nmslab * settings.slab_size;
    mstart = fc_mmap(mspace);
    if (mstart == NULL) {
        log_error("mmap %zu bytes failed: %s", mspace, strerror(errno));
        return FC_ENOMEM;
    }
    mend = mstart + mspace;

    /* init ndslab, dstart and dend */
    status = fc_device_size(settings.ssd_device, &size);
    if (status != FC_OK) {
        return status;
    }
    ndchunk = size / settings.slab_size;
    ASSERT(settings.server_n <= ndchunk);
    ndslab = ndchunk / settings.server_n;
    dspace = ndslab * settings.slab_size;
    dstart = (settings.server_id * ndslab) * settings.slab_size;
    dend = ((settings.server_id + 1) * ndslab) * settings.slab_size;

    /* init disk descriptor */
    fd = open(settings.ssd_device, O_RDWR | O_DIRECT, 0644);
    if (fd < 0) {
        log_error("open '%s' failed: %s", settings.ssd_device, strerror(errno));
        return FC_ERROR;
    }

    /* init slab table */
    status = slab_init_stable();
    if (status != FC_OK) {
        return status;
    }

    /* init evictbuf and readbuf */
    evictbuf = fc_mmap(settings.slab_size);
    if (evictbuf == NULL) {
        log_error("mmap %zu bytes failed: %s", settings.slab_size,
            strerror(errno));
        return FC_ENOMEM;
    }
    memset(evictbuf, 0xff, settings.slab_size);

    readbuf = fc_mmap(settings.slab_size);
    if (readbuf == NULL) {
        log_error("mmap %zu bytes failed: %s", settings.slab_size,
            strerror(errno));
        return FC_ENOMEM;
    }
    memset(readbuf, 0xff, settings.slab_size);

    //init lru controller
    lruh = (lru_head*)malloc(sizeof(lru_head));
    lruh_disk = (lru_head*)malloc(sizeof(lru_head));
    lruh->head = NULL;
    lruh->tail = NULL;
    lruh_disk->head = NULL;
    lruh_disk->tail = NULL;

    return FC_OK;
}

void slab_deinit(void)
{
    slab_deinit_ctable();
    slab_deinit_stable();
}

uint32_t
slab_msinfo_nalloc(void)
{
    return nmslab;
}

uint32_t
slab_msinfo_nfree(void)
{
    return nfree_msinfoq;
}

uint32_t
slab_msinfo_nfull(void)
{
    return nfull_msinfoq;
}

uint32_t
slab_msinfo_npartial(void)
{
    return nmslab - nfree_msinfoq - nfull_msinfoq;
}

uint32_t
slab_dsinfo_nalloc(void)
{
    return ndslab;
}

uint32_t
slab_dsinfo_nfree(void)
{
    return nfree_dsinfoq;
}

uint32_t
slab_dsinfo_nfull(void)
{
    return nfull_dsinfoq;
}

uint64_t
slab_nevict(void)
{
    return nevict;
}

uint64_t
slab_nflsuh(void)
{
    return nflush;
}

uint8_t
slab_max_cid(void)
{
    return nctable;
}

uint8_t
slab_get_cid(uint32_t sid)
{
    ASSERT(sid < nstable);
    return stable[sid].cid;
}

struct slabclass*
slab_get_class_by_cid(uint8_t cid)
{
    if (cid > nctable) {
        return NULL;
    }
    return &ctable[cid];
}

bool slab_incr_chunks_by_sid(uint32_t sid, int n)
{
    struct slabinfo* sinfo;
    struct slabclass* c;

    sinfo = &stable[sid];
    if (sinfo == NULL) {
        return false;
    }
    c = &ctable[sinfo->cid];
    c->nused_item += n;
    return true;
}

void lru_set(lru_head* lru, struct slabinfo* item)
{   
    if (lru->head == NULL && lru->tail == NULL) { //fully empty lru list case
        lru->head = item;
        lru->tail = item;
        item->pre = NULL;
        item->next = NULL;
    } else {
        struct slabinfo* old_pre = item->pre;
        struct slabinfo* old_next = item->next;
        struct slabinfo* old_tail = lru->tail;

        if (old_pre && !old_next) { //the last one, don't have to change
            return;
        }

        item->pre = old_tail;
        item->next = NULL;
        lru->tail = item;
        old_tail->next = item;

        if (!old_pre && old_next) { //means the first one
            lru->head = old_next;
            old_next->pre = NULL;
        } else if (old_pre && old_next) { //between two items
            old_pre->next = old_next;
            old_next->pre = old_pre;
        }
    }
}

void lru_remove_head(lru_head* lruh)
{   
    ASSERT(lruh->head);
    log_debug(LOG_DEBUG, "lru remove sid:%d, after remove:", lruh->head->sid);
    struct slabinfo* new_head = lruh->head->next;
    if (new_head) {
        lruh->head->next = NULL;
        lruh->head = new_head;
        new_head->pre = NULL;
    } else {
        lruh->head = NULL;
        lruh->tail = NULL;
    }
    struct slabinfo* tmp = lruh->head;
    while (tmp){
        log_debug(LOG_DEBUG, "lru:%d,", tmp->sid);
        tmp = tmp->next;
    }

}