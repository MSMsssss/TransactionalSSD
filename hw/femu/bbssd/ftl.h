#ifndef __FEMU_FTL_H
#define __FEMU_FTL_H

#include "../nvme.h"


#define INVALID_PPA     (~(0ULL))
#define INVALID_LPN     (~(0ULL))
#define UNMAPPED_PPA    (~(0ULL))
#define MAX_TIME_STAMP  (~(0ULL))
#define MAX_LPN_PER_TX  (2048)
#define MAX_TX_NUM      (256)
#define INVALID_TX_ID   (~(0U))
#define INVALID_MAP_DATA_INDEX (~(0U))
#define IN_USE_FLAG     (1)
#define UN_USE_FLAG     (0)
#define PPA_IS_INDEX_FLAG (1)
#define MAX_LENGTH_OF_META_DATA_LIST (65536)
#define INDEX_BITS (32)

#define TX_TIME_OUT_VALUE (30000)    //3000ms

enum {
    NAND_READ =  0,
    NAND_WRITE = 1,
    NAND_ERASE = 2,

    NAND_READ_LATENCY = 40000,
    NAND_PROG_LATENCY = 200000,
    NAND_ERASE_LATENCY = 2000000,
};

enum {
    USER_IO = 0,
    GC_IO = 1,
};

enum {
    SEC_FREE = 0,
    SEC_INVALID = 1,
    SEC_VALID = 2,

    PG_FREE = 0,
    PG_INVALID = 1,
    PG_VALID = 2,
    PG_UNCOMMITTED = 3,
    PG_OLD_VERSION = 4,
};

enum {
    FEMU_ENABLE_GC_DELAY = 1,
    FEMU_DISABLE_GC_DELAY = 2,

    FEMU_ENABLE_DELAY_EMU = 3,
    FEMU_DISABLE_DELAY_EMU = 4,

    FEMU_RESET_ACCT = 5,
    FEMU_ENABLE_LOG = 6,
    FEMU_DISABLE_LOG = 7,
};

enum {
    TX_INIT = 0,
    TX_COMMIT = 1,
    TX_ABORT = 2
};

enum {
    TX_MAP_DATA_INVALID = 0,
    TX_MAP_DATA_UNCOMMITTED = 1,
    TX_MAP_DATA_COMMITED = 2,
};

/* error code is negative value and status code is positive value */
enum {
    TX_ERROR_ABORT = 1,
    TX_ERROR_NO_BUF = 2,
    TX_ERROR_DATA_REPEAT = 3,
    TX_ERROR_READ_UNCOMMITTED = 4,
    TX_ERROR_READ_NULL = 5,
};

enum {
    TX_STATUS_OK = 0,
};

#define BLK_BITS    (16)
#define PG_BITS     (16)
#define SEC_BITS    (8)
#define PL_BITS     (8)
#define LUN_BITS    (8)
#define CH_BITS     (7)

/* describe a physical page addr */
struct ppa {
    union {
        struct {
            uint64_t blk : BLK_BITS;
            uint64_t pg  : PG_BITS;
            uint64_t sec : SEC_BITS;
            uint64_t pl  : PL_BITS;
            uint64_t lun : LUN_BITS;
            uint64_t ch  : CH_BITS;
            uint64_t isIndex : 1;
        } g;

        struct {
            uint64_t index : INDEX_BITS;
            uint64_t rsv   : 31;
            uint64_t isIndex : 1;
        } idx;
        
        uint64_t ppa;
    };
};

typedef int nand_sec_status_t;

struct nand_page {
    nand_sec_status_t *sec;
    int nsecs;
    int status;
    uint32_t map_data_index;   // It can be ignore if status is not UNCOMMITTED or OLD_VERSION
};

struct nand_block {
    struct nand_page *pg;
    int npgs;
    int ipc; /* invalid page count */
    int vpc; /* valid page count */
    int erase_cnt;
    int wp; /* current write pointer */
};

struct nand_plane {
    struct nand_block *blk;
    int nblks;
};

struct nand_lun {
    struct nand_plane *pl;
    int npls;
    uint64_t next_lun_avail_time;
    bool busy;
    uint64_t gc_endtime;
};

struct ssd_channel {
    struct nand_lun *lun;
    int nluns;
    uint64_t next_ch_avail_time;
    bool busy;
    uint64_t gc_endtime;
};

struct ssdparams {
    int secsz;        /* sector size in bytes */
    int secs_per_pg;  /* # of sectors per page */
    int pgs_per_blk;  /* # of NAND pages per block */
    int blks_per_pl;  /* # of blocks per plane */
    int pls_per_lun;  /* # of planes per LUN (Die) */
    int luns_per_ch;  /* # of LUNs per channel */
    int nchs;         /* # of channels in the SSD */

    int pg_rd_lat;    /* NAND page read latency in nanoseconds */
    int pg_wr_lat;    /* NAND page program latency in nanoseconds */
    int blk_er_lat;   /* NAND block erase latency in nanoseconds */
    int ch_xfer_lat;  /* channel transfer latency for one page in nanoseconds
                       * this defines the channel bandwith
                       */

    double gc_thres_pcent;
    int gc_thres_lines;
    double gc_thres_pcent_high;
    int gc_thres_lines_high;
    bool enable_gc_delay;

    /* below are all calculated values */
    int secs_per_blk; /* # of sectors per block */
    int secs_per_pl;  /* # of sectors per plane */
    int secs_per_lun; /* # of sectors per LUN */
    int secs_per_ch;  /* # of sectors per channel */
    int tt_secs;      /* # of sectors in the SSD */

    int pgs_per_pl;   /* # of pages per plane */
    int pgs_per_lun;  /* # of pages per LUN (Die) */
    int pgs_per_ch;   /* # of pages per channel */
    int tt_pgs;       /* total # of pages in the SSD */

    int blks_per_lun; /* # of blocks per LUN */
    int blks_per_ch;  /* # of blocks per channel */
    int tt_blks;      /* total # of blocks in the SSD */

    int secs_per_line;
    int pgs_per_line;
    int blks_per_line;
    int tt_lines;

    int pls_per_ch;   /* # of planes per channel */
    int tt_pls;       /* total # of planes in the SSD */

    int tt_luns;      /* total # of LUNs in the SSD */
};

typedef struct line {
    int id;  /* line id, the same as corresponding block id */
    int ipc; /* invalid page count in this line */
    int vpc; /* valid page count in this line */
    QTAILQ_ENTRY(line) entry; /* in either {free,victim,full} list */
    /* position in the priority queue for victim lines */
    size_t                  pos;
} line;

/* wp: record next write addr */
struct write_pointer {
    struct line *curline;
    int ch;
    int lun;
    int pg;
    int blk;
    int pl;
};

struct line_mgmt {
    struct line *lines;
    /* free line list, we only need to maintain a list of blk numbers */
    QTAILQ_HEAD(free_line_list, line) free_line_list;
    pqueue_t *victim_line_pq;
    //QTAILQ_HEAD(victim_line_list, line) victim_line_list;
    QTAILQ_HEAD(full_line_list, line) full_line_list;
    int tt_lines;
    int free_line_cnt;
    int victim_line_cnt;
    int full_line_cnt;
};

struct nand_cmd {
    int type;
    int cmd;
    int64_t stime; /* Coperd: request arrival time */
};

typedef struct map_data {
    uint16_t status;    // committed, uncommitted, invalid
    uint64_t read_ts;   // last read ts
    uint64_t write_ts;  // this version create ts

    struct ppa next;
    struct ppa *prev_field;

    uint64_t lpn;    // logic page 4K
    struct ppa ppn;  // phy page
    size_t pos; // used by pq
} map_data;

/* transcation meta data entry */
typedef struct tx_table_entry {
    int64_t start_time; // this entry alloc time, free when timeout
    uint64_t tx_timestamp;
    uint32_t in_used;
    int32_t tx_id;
    int32_t status;
    uint32_t map_data_index_array[MAX_LPN_PER_TX];
    int32_t lpn_count;   
} tx_table_entry;

typedef struct tx_map_result_enrty {
    struct ppa ppa;
    uint64_t *target_read_ts;
} tx_map_result_enrty;

struct ssd {
    char *ssdname;
    struct ssdparams sp;
    struct ssd_channel *ch;
    struct ppa *maptbl; /* page level mapping table */
    uint64_t* read_ts_table;
    uint64_t *rmap;     /* reverse mapptbl, assume it's stored in OOB */
    struct write_pointer wp;
    struct line_mgmt lm;

    /* tx module data */
    tx_table_entry* tx_table;
    map_data* map_data_table;
    idx_pool* tx_idx_pool;
    idx_pool* map_data_idx_pool;
    int64_t check_tx_timeout_timer;
    uint64_t min_ts_active;
    tx_map_result_enrty *read_map_buffer;
    double old_version_gc_threshold;
    pqueue_t *committed_queue;

    /* lockless ring for communication with NVMe IO thread */
    struct rte_ring **to_ftl;
    struct rte_ring **to_poller;
    bool *dataplane_started_ptr;
    QemuThread ftl_thread;
};

void ssd_init(FemuCtrl *n);

#ifdef FEMU_DEBUG_FTL
#define ftl_debug(fmt, ...) \
    do { printf("[FEMU] FTL-Dbg: " fmt, ## __VA_ARGS__); } while (0)
#else
#define ftl_debug(fmt, ...) \
    do { } while (0)
#endif

#define ftl_err(fmt, ...) \
    do { fprintf(stderr, "[FEMU] FTL-Err: " fmt, ## __VA_ARGS__); } while (0)

#define ftl_log(fmt, ...) \
    do { printf("[FEMU] FTL-Log: " fmt, ## __VA_ARGS__); } while (0)


/* FEMU assert() */
#ifdef FEMU_DEBUG_FTL
#define ftl_assert(expression) assert(expression)
#else
#define ftl_assert(expression)
#endif

#endif
