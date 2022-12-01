#include "ftl.h"

//#define FEMU_DEBUG_FTL

static void *ftl_thread(void *arg);
static uint64_t __ssd_commit(struct ssd *ssd, uint32_t txid);
static uint64_t __ssd_abort(struct ssd *ssd, uint32_t txid);
static inline struct map_data* get_version_data_from_ppa(struct ssd* ssd, struct ppa* ppa);
static inline uint64_t get_map_data_entry_index(struct ssd *ssd, struct map_data *entry);
static bool map_entry_is_index(struct ppa* ppa);

static inline bool should_gc(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines);
}

static inline bool should_gc_high(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines_high);
}

static inline struct ppa* get_maptbl_ent_pointer(struct ssd* ssd, uint64_t lpn) {
    return &ssd->maptbl[lpn];
}

static inline struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn)
{
    return ssd->maptbl[lpn];
}

static inline void set_maptbl_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    ftl_assert(lpn < ssd->sp.tt_pgs);
    ssd->maptbl[lpn] = *ppa;
}

static uint64_t ppa2pgidx(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t pgidx;

    pgidx = ppa->g.ch  * spp->pgs_per_ch  + \
            ppa->g.lun * spp->pgs_per_lun + \
            ppa->g.pl  * spp->pgs_per_pl  + \
            ppa->g.blk * spp->pgs_per_blk + \
            ppa->g.pg;

    ftl_assert(pgidx < spp->tt_pgs);

    return pgidx;
}

static inline uint64_t get_rmap_ent(struct ssd *ssd, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    ssd->rmap[pgidx] = lpn;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
    return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
    return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
    ((struct line *)a)->pos = pos;
}

static inline int map_data_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t map_data_get_pri(void *a)
{
    return (pqueue_pri_t)(((map_data *)a)->write_ts);
}

static inline void map_data_set_pri(void *a, pqueue_pri_t pri)
{
    ((map_data *)a)->write_ts = (uint64_t)pri;
}

static inline size_t map_data_get_pos(void *a) {
    return ((map_data*)a)->pos;
}

static inline void map_data_set_pos(void *a, size_t pos) {
    ((map_data*)a)->pos = pos;
}

static void ssd_init_lines(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *line;

    lm->tt_lines = spp->blks_per_pl;
    ftl_assert(lm->tt_lines == spp->tt_lines);
    lm->lines = g_malloc0(sizeof(struct line) * lm->tt_lines);

    QTAILQ_INIT(&lm->free_line_list);
    lm->victim_line_pq = pqueue_init(spp->tt_lines, victim_line_cmp_pri,
            victim_line_get_pri, victim_line_set_pri,
            victim_line_get_pos, victim_line_set_pos);
    QTAILQ_INIT(&lm->full_line_list);

    lm->free_line_cnt = 0;
    for (int i = 0; i < lm->tt_lines; i++) {
        line = &lm->lines[i];
        line->id = i;
        line->ipc = 0;
        line->vpc = 0;
        line->pos = 0;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
        lm->free_line_cnt++;
    }

    ftl_assert(lm->free_line_cnt == lm->tt_lines);
    lm->victim_line_cnt = 0;
    lm->full_line_cnt = 0;
}

static void ssd_init_write_pointer(struct ssd *ssd)
{
    struct write_pointer *wpp = &ssd->wp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;

    /* wpp->curline is always our next-to-write super-block */
    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = 0;
    wpp->pl = 0;
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

static struct line *get_next_free_line(struct ssd *ssd)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        ftl_err("No free lines left in [%s] !!!!\n", ssd->ssdname);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    return curline;
}

static void ssd_advance_write_pointer(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp = &ssd->wp;
    struct line_mgmt *lm = &ssd->lm;

    check_addr(wpp->ch, spp->nchs);
    wpp->ch++;
    if (wpp->ch == spp->nchs) {
        wpp->ch = 0;
        check_addr(wpp->lun, spp->luns_per_ch);
        wpp->lun++;
        /* in this case, we should go to next lun */
        if (wpp->lun == spp->luns_per_ch) {
            wpp->lun = 0;
            /* go to next page in the block */
            check_addr(wpp->pg, spp->pgs_per_blk);
            wpp->pg++;
            if (wpp->pg == spp->pgs_per_blk) {
                wpp->pg = 0;
                /* move current line to {victim,full} line list */
                if (wpp->curline->vpc == spp->pgs_per_line) {
                    /* all pgs are still valid, move to full line list */
                    ftl_assert(wpp->curline->ipc == 0);
                    QTAILQ_INSERT_TAIL(&lm->full_line_list, wpp->curline, entry);
                    lm->full_line_cnt++;
                } else {
                    ftl_assert(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
                    /* there must be some invalid pages in this line */
                    ftl_assert(wpp->curline->ipc > 0);
                    pqueue_insert(lm->victim_line_pq, wpp->curline);
                    lm->victim_line_cnt++;
                }
                /* current line is used up, pick another empty line */
                check_addr(wpp->blk, spp->blks_per_pl);
                wpp->curline = NULL;
                wpp->curline = get_next_free_line(ssd);
                if (!wpp->curline) {
                    /* TODO */
                    abort();
                }
                wpp->blk = wpp->curline->id;
                check_addr(wpp->blk, spp->blks_per_pl);
                /* make sure we are starting from page 0 in the super block */
                ftl_assert(wpp->pg == 0);
                ftl_assert(wpp->lun == 0);
                ftl_assert(wpp->ch == 0);
                /* TODO: assume # of pl_per_lun is 1, fix later */
                ftl_assert(wpp->pl == 0);
            }
        }
    }
}

static struct ppa get_new_page(struct ssd *ssd)
{
    struct write_pointer *wpp = &ssd->wp;
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.lun = wpp->lun;
    ppa.g.pg = wpp->pg;
    ppa.g.blk = wpp->blk;
    ppa.g.pl = wpp->pl;
    ftl_assert(ppa.g.pl == 0);

    return ppa;
}

static void check_params(struct ssdparams *spp)
{
    /*
     * we are using a general write pointer increment method now, no need to
     * force luns_per_ch and nchs to be power of 2
     */

    //ftl_assert(is_power_of_2(spp->luns_per_ch));
    //ftl_assert(is_power_of_2(spp->nchs));
}

static void ssd_init_params(struct ssdparams *spp)
{
    spp->secsz = 512;
    spp->secs_per_pg = 8;
    spp->pgs_per_blk = 256;
    spp->blks_per_pl = 256; /* 16GB */
    spp->pls_per_lun = 1;
    spp->luns_per_ch = 8;
    spp->nchs = 8;

    spp->pg_rd_lat = NAND_READ_LATENCY;
    spp->pg_wr_lat = NAND_PROG_LATENCY;
    spp->blk_er_lat = NAND_ERASE_LATENCY;
    spp->ch_xfer_lat = 0;

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    /* line is special, put it at the end */
    spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
    spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
    spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
    spp->tt_lines = spp->blks_per_lun; /* TODO: to fix under multiplanes */

    spp->gc_thres_pcent = 0.75;
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->gc_thres_pcent_high = 0.95;
    spp->gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_lines);
    spp->enable_gc_delay = true;


    check_params(spp);
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (int i = 0; i < pl->nblks; i++) {
        ssd_init_nand_blk(&pl->blk[i], spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++) {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++) {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

static void ssd_init_maptbl(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_rmap(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->rmap = g_malloc0(sizeof(uint64_t) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->rmap[i] = INVALID_LPN;
    }
}

static void ssd_init_tx_module(struct ssd* ssd) {
    ssd->tx_table = g_malloc0(sizeof(tx_table_entry) * MAX_TX_NUM);
    ssd->tx_idx_pool = idx_pool_create(MAX_TX_NUM);
    ssd->map_data_idx_pool = idx_pool_create(MAX_LENGTH_OF_META_DATA_LIST);
    ssd->map_data_table = g_malloc0(sizeof(map_data) * MAX_LENGTH_OF_META_DATA_LIST);
    ssd->min_ts_active = 0;
    ssd->read_ts_table = g_malloc0(sizeof(uint64_t) * ssd->sp.tt_pgs);
    ssd->read_map_buffer = g_malloc0(sizeof(tx_map_result_enrty) * MAX_LPN_PER_TX);
    ssd->old_version_gc_threshold = 0.8;
    ssd->committed_queue = pqueue_init(MAX_LENGTH_OF_META_DATA_LIST, map_data_cmp_pri, 
                                       map_data_get_pri, map_data_set_pri, 
                                       map_data_get_pos, map_data_set_pos);

    if (ssd->tx_table == NULL) {
        femu_log("tx table init failed");
    }

    if (ssd->tx_idx_pool == NULL) {
        femu_log("idx pool init failed");
    }

    if (ssd->map_data_table == NULL) {
        femu_log("map data table init failed");
    }

    if (ssd->map_data_idx_pool == NULL) {
        femu_log("map data idx pool init failed");
    }

    for (int i = 0; i < MAX_LENGTH_OF_META_DATA_LIST; i++) {
        ssd->map_data_table[i].status = TX_MAP_DATA_INVALID;
    }
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    ssd_init_params(spp);

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize all the lines */
    ssd_init_lines(ssd);

    /* initialize write pointer, this is how we allocate new pages for writes */
    ssd_init_write_pointer(ssd);

    /* initialize transcation module */
    ssd_init_tx_module(ssd);

    qemu_thread_create(&ssd->ftl_thread, "FEMU-FTL-Thread", ftl_thread, n,
                       QEMU_THREAD_JOINABLE);
}

static inline bool valid_ppa(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    int ch = ppa->g.ch;
    int lun = ppa->g.lun;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sec = ppa->g.sec;

    if (ch >= 0 && ch < spp->nchs && lun >= 0 && lun < spp->luns_per_ch && pl >=
        0 && pl < spp->pls_per_lun && blk >= 0 && blk < spp->blks_per_pl && pg
        >= 0 && pg < spp->pgs_per_blk && sec >= 0 && sec < spp->secs_per_pg)
        return true;

    return false;
}

static inline bool valid_lpn(struct ssd *ssd, uint64_t lpn)
{
    return (lpn < ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct line *get_line(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->lm.lines[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct
        nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t lat = 0;

    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
#if 0
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;

        /* read: then data transfer through channel */
        chnl_stime = (ch->next_ch_avail_time < lun->next_lun_avail_time) ? \
            lun->next_lun_avail_time : ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        lat = ch->next_ch_avail_time - cmd_stime;
#endif
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        if (ncmd->type == USER_IO) {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        } else {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        }
        lat = lun->next_lun_avail_time - cmd_stime;

#if 0
        chnl_stime = (ch->next_ch_avail_time < cmd_stime) ? cmd_stime : \
                     ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        /* write: then do NAND program */
        nand_stime = (lun->next_lun_avail_time < ch->next_ch_avail_time) ? \
            ch->next_ch_avail_time : lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
#endif
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        ftl_err("Unsupported NAND command: 0x%x\n", c);
    }

    return lat;
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    bool was_full_line = false;
    struct line *line;

    /* update corresponding page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_VALID || pg->status == PG_UNCOMMITTED pg->status == PG_OLD_VERSION);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
    blk->ipc++;
    ftl_assert(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    blk->vpc--;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    ftl_assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    if (line->vpc == spp->pgs_per_line) {
        ftl_assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    ftl_assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    /* Adjust the position of the victime line in the pq under over-writes */
    if (line->pos) {
        /* Note that line->vpc will be updated by this call */
        pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
    } else {
        line->vpc--;
    }

    if (was_full_line) {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }
}

static void mark_page_valid(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ssd->sp.pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    ftl_assert(line->vpc >= 0 && line->vpc < ssd->sp.pgs_per_line);
    line->vpc++;
}

static void mark_page_uncommitted(struct ssd *ssd, struct ppa *ppa, uint32_t map_data_index)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_UNCOMMITTED;
    pg->map_data_index = map_data_index;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ssd->sp.pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    ftl_assert(line->vpc >= 0 && line->vpc < ssd->sp.pgs_per_line);
    line->vpc++;
}

static void mark_block_free(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < spp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == spp->secs_per_pg);
        pg->status = PG_FREE;
    }

    /* reset block status */
    ftl_assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt++;
}

static void gc_read_page(struct ssd *ssd, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        ssd_advance_status(ssd, ppa, &gcr);
    }
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct ssd *ssd, struct ppa *old_ppa)
{
    struct ppa new_ppa;
    struct nand_lun *new_lun;
    struct nand_page* page = get_pg(ssd, old_ppa);
    uint64_t lpn;
    struct map_data *map_info, *version_ptr = NULL;
    struct ppa *ppa_ptr = NULL;

    ftl_assert(valid_lpn(ssd, lpn));
    /* get a new free page */
    new_ppa = get_new_page(ssd);

    if (page->status == PG_VALID) {
        lpn = get_rmap_ent(ssd, old_ppa);
        ppa_ptr = get_maptbl_ent_pointer(ssd, lpn);
        while (map_entry_is_index(ppa_ptr)) {
            version_ptr = get_version_data_from_ppa(ssd, ppa_ptr);
            ppa_ptr = &version_ptr->next;
        }

        *ppa_ptr = new_ppa;
        /* update rmap */
        set_rmap_ent(ssd, lpn, &new_ppa);
        mark_page_valid(ssd, &new_ppa);
    } else if (page->status == PG_UNCOMMITTED || page->status == PG_OLD_VERSION) {
        map_info = &ssd->map_data_table[page->map_data_index];
        lpn = map_info->lpn;

        /* update rmap*/
        set_rmap_ent(ssd, lpn, &new_ppa);
        /* update tx meta data */
        map_info->ppn = new_ppa;
        mark_page_uncommitted(ssd, &new_ppa, page->map_data_index);
        get_pg(ssd, &new_ppa)->status = page->status;
    } else {
        femu_log("run to err branch");
        return 0;
    }

    /* need to advance the write pointer here */
    ssd_advance_write_pointer(ssd);

    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ssd, &new_ppa, &gcw);
    }

    /* advance per-ch gc_endtime as well */
#if 0
    new_ch = get_ch(ssd, &new_ppa);
    new_ch->gc_endtime = new_ch->next_ch_avail_time;
#endif

    new_lun = get_lun(ssd, &new_ppa);
    new_lun->gc_endtime = new_lun->next_lun_avail_time;

    return 0;
}

static struct line *select_victim_line(struct ssd *ssd, bool force)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *victim_line = NULL;

    victim_line = pqueue_peek(lm->victim_line_pq);
    if (!victim_line) {
        return NULL;
    }

    if (!force && victim_line->ipc < ssd->sp.pgs_per_line / 8) {
        return NULL;
    }

    pqueue_pop(lm->victim_line_pq);
    victim_line->pos = 0;
    lm->victim_line_cnt--;

    /* victim_line is a danggling node now */
    return victim_line;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    for (int pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ssd, ppa);
        /* there shouldn't be any free page in victim blocks */
        ftl_assert(pg_iter->status != PG_FREE);
        if (pg_iter->status == PG_VALID || pg_iter->status == PG_UNCOMMITTED || pg_iter->status == PG_OLD_VERSION) {
            gc_read_page(ssd, ppa);
            /* delay the maptbl update until "write" happens */
            gc_write_page(ssd, ppa);
            cnt++;
        }
    }

    ftl_assert(get_blk(ssd, ppa)->vpc == cnt);
}

static void mark_line_free(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *line = get_line(ssd, ppa);
    line->ipc = 0;
    line->vpc = 0;
    /* move this line to free line list */
    QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
    lm->free_line_cnt++;
}

static int do_gc(struct ssd *ssd, bool force)
{
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lunp;
    struct ppa ppa;
    int ch, lun;

    victim_line = select_victim_line(ssd, force);
    if (!victim_line) {
        return -1;
    }

    ppa.g.blk = victim_line->id;
    ftl_debug("GC-ing line:%d,ipc=%d,victim=%d,full=%d,free=%d\n", ppa.g.blk,
              victim_line->ipc, ssd->lm.victim_line_cnt, ssd->lm.full_line_cnt,
              ssd->lm.free_line_cnt);

    /* copy back valid data */
    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            lunp = get_lun(ssd, &ppa);
            clean_one_block(ssd, &ppa);
            mark_block_free(ssd, &ppa);

            if (spp->enable_gc_delay) {
                struct nand_cmd gce;
                gce.type = GC_IO;
                gce.cmd = NAND_ERASE;
                gce.stime = 0;
                ssd_advance_status(ssd, &ppa, &gce);
            }

            lunp->gc_endtime = lunp->next_lun_avail_time;
        }
    }

    /* update line status */
    mark_line_free(ssd, &ppa);

    return 0;
}

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ssd, lpn);
        if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) {
            //printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            //printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            //ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            continue;
        }

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct ssdparams *spp = &ssd->sp;
    int len = req->nlb;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    while (should_gc_high(ssd)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ssd, true);
        if (r == -1)
            break;
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ssd, lpn);
        if (mapped_ppa(&ppa)) {
            /* update old page information first */
            mark_page_invalid(ssd, &ppa);
            set_rmap_ent(ssd, INVALID_LPN, &ppa);
        }

        /* new write */
        ppa = get_new_page(ssd);
        /* update maptbl */
        set_maptbl_ent(ssd, lpn, &ppa);
        /* update rmap */
        set_rmap_ent(ssd, lpn, &ppa);

        mark_page_valid(ssd, &ppa);

        /* need to advance the write pointer here */
        ssd_advance_write_pointer(ssd);

        struct nand_cmd swr;
        swr.type = USER_IO;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        /* get latency statistics */
        curlat = ssd_advance_status(ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

static bool map_entry_is_index(struct ppa* ppa) {
    if (!mapped_ppa(ppa)) {
        return false;
    }

    return ppa->idx.isIndex == PPA_IS_INDEX_FLAG;
}

static inline bool entry_in_use(tx_table_entry* entry) {
    return entry->in_used == IN_USE_FLAG;
}

static inline void set_entry_unused(tx_table_entry* entry) {
    entry->in_used = UN_USE_FLAG;
}

static inline void set_entry_inused(tx_table_entry* entry) {
    entry->in_used = IN_USE_FLAG;
}

static void check_timeout_tx(struct ssd *ssd) {
    int64_t cur_time = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    tx_table_entry* tx_table = ssd->tx_table;
    int64_t delta_time = cur_time - ssd->check_tx_timeout_timer;
    
    // check once per TX_TIME_OUT_VALUE
    if (delta_time < TX_TIME_OUT_VALUE) {
        return;
    }

    ssd->check_tx_timeout_timer = cur_time;
    for (int i = 0; i < MAX_TX_NUM; i++) {
        if (entry_in_use(&tx_table[i]) && tx_table[i].status == TX_INIT) {
            delta_time = cur_time - tx_table[i].start_time;
            if (delta_time > TX_TIME_OUT_VALUE) {
                __ssd_abort(ssd, tx_table[i].tx_id);
            }
        }
    }

    return;
}

static uint64_t ssd_begin(struct ssd *ssd, NvmeRequest *req) {
    NvmeTxAdminCmd* cmd = (NvmeTxAdminCmd*)&req->cmd;
    int ret;
    ret = idx_pool_alloc(ssd->tx_idx_pool);
    if (ret < 0) {
        // tx table is full
        req->cqe.n.result = cpu_to_le32(INVALID_TX_ID);
    } else {
        // return tx_id to host
        req->cqe.n.result = cpu_to_le32((uint32_t)ret);
        ssd->tx_table[ret].lpn_count = 0;
        ssd->tx_table[ret].tx_id = ret;
        ssd->tx_table[ret].status = TX_INIT;
        ssd->tx_table[ret].start_time = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
        ssd->tx_table[ret].tx_timestamp = le64_to_cpu(cmd->timestamp);
        set_entry_inused(&ssd->tx_table[ret]);
    }

    femu_debug("begin with idx %d", ret);

    return 0;
}

static int ssd_tread_precheck_one_lpn(struct ssd *ssd, uint64_t lpn, tx_map_result_enrty *result, uint64_t read_ts) {
    struct map_data *version_ptr;
    struct ppa *header_ptr = get_maptbl_ent_pointer(ssd, lpn), *ppa_ptr = NULL;

    if (!map_entry_is_index(header_ptr)) {
        result->ppa = *header_ptr;
        result->target_read_ts = &ssd->read_ts_table[lpn];
        return TX_STATUS_OK;
    }

    ppa_ptr = header_ptr;
    version_ptr = get_version_data_from_ppa(ssd, ppa_ptr);
    while (read_ts < version_ptr->write_ts) {
        ppa_ptr = &version_ptr->next;
        if (map_entry_is_index(ppa_ptr)) {
            version_ptr = get_version_data_from_ppa(ssd, ppa_ptr);
        } else {
            version_ptr = NULL;
            break;
        }
    }

    if (version_ptr != NULL) {
        /* read req of one tx can get the data without committed  */
        if (version_ptr->write_ts != read_ts && version_ptr->status != TX_MAP_DATA_COMMITED) {
            return -TX_ERROR_READ_UNCOMMITTED;
        }

        result->ppa = version_ptr->ppn;
        result->target_read_ts = &version_ptr->read_ts;
    } else {
        result->ppa = *ppa_ptr;
        result->target_read_ts = &ssd->read_ts_table[lpn];
    }

    return TX_STATUS_OK;
}

static int ssd_tread_precheck(struct ssd *ssd, NvmeRequest *req) {
    NvmeTxReadCmd* cmd = (NvmeTxReadCmd*)&req->cmd;
    struct ssdparams *spp = &ssd->sp;
    int len  = le16_to_cpu(cmd->nlb) + 1;
    uint64_t lba = le64_to_cpu(cmd->slba);
    uint64_t read_ts = le64_to_cpu(cmd->timestamp);
    tx_map_result_enrty *search_result = NULL;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    uint64_t lpn;
    int ret;

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        search_result = &ssd->read_map_buffer[lpn - start_lpn];
        ret = ssd_tread_precheck_one_lpn(ssd, lpn, search_result, read_ts);
        if (ret == -TX_ERROR_READ_UNCOMMITTED) {
            return -TX_ERROR_READ_UNCOMMITTED;
        }
    }

    return TX_STATUS_OK;
}

static uint64_t ssd_tread(struct ssd *ssd, NvmeRequest *req)
{
    NvmeTxReadCmd* cmd = (NvmeTxReadCmd*)&req->cmd;
    int len  = le16_to_cpu(cmd->nlb) + 1;
    uint64_t lba = le64_to_cpu(cmd->slba);
    uint64_t read_ts = le64_to_cpu(cmd->timestamp);
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    tx_map_result_enrty *search_result = NULL;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;
    int ret;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }
    
    /* pre check read uncommitted before data transfer */
    req->cqe.n.result = cpu_to_le32(TX_STATUS_OK);
    ret = ssd_tread_precheck(ssd, req);
    if (ret == -TX_ERROR_READ_UNCOMMITTED) {
        req->cqe.n.result = cpu_to_le32(TX_ERROR_READ_UNCOMMITTED);
        return 0;
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        search_result = &ssd->read_map_buffer[lpn - start_lpn];
        ppa = search_result->ppa;

        if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) {
            //printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            //printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            //ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            femu_log("read no data lpn");
            continue;
        }

        if (read_ts > *(search_result->target_read_ts)) {
            *(search_result->target_read_ts) = read_ts;
        }

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

static inline struct map_data* get_version_data_from_ppa(struct ssd* ssd, struct ppa* ppa) {
    return &ssd->map_data_table[ppa->idx.index];
}

static inline void set_version_index(struct ppa* ppa, uint64_t index) {
    ppa->ppa = 0;
    ppa->idx.isIndex = PPA_IS_INDEX_FLAG;
    ppa->idx.index = index;
}

static inline uint64_t get_map_data_entry_index(struct ssd *ssd, struct map_data *entry) {
    return (uint64_t)(entry - ssd->map_data_table);
}

static int ssd_free_map_data_entry(struct ssd *ssd, struct map_data *free_data) {
    if (free_data->status == TX_MAP_DATA_INVALID) {
        femu_log("repeat free map data entry");
        return TX_STATUS_OK;
    }

    free_data->status = TX_MAP_DATA_INVALID;
    idx_pool_free(ssd->map_data_idx_pool, (int)get_map_data_entry_index(ssd, free_data));

    return TX_STATUS_OK;
}

static int ssd_delete_data_version(struct ssd *ssd, struct map_data *delete_version) {
    if (delete_version == NULL) {
        femu_log("delete nullptr");
        return -TX_ERROR_READ_NULL;
    }

    struct ppa *prev_field = delete_version->prev_field;
    struct map_data *next_version = NULL;

    if (map_entry_is_index(&delete_version->next)) {
        next_version = get_version_data_from_ppa(ssd, &delete_version->next);
        next_version->prev_field = prev_field;
    }

    *prev_field = delete_version->next;

    return TX_STATUS_OK;
}

static int ssd_insert_data_version(struct ssd* ssd, uint64_t lpn, struct map_data* new_version, struct map_data** repeat_version) {
    struct ppa *header_ppa = get_maptbl_ent_pointer(ssd, lpn), *ppa_ptr = NULL;
    uint64_t new_version_idx = get_map_data_entry_index(ssd, new_version);
    uint64_t last_read_ts = ssd->read_ts_table[lpn];
    struct map_data* version_ptr = NULL;

    if (!map_entry_is_index(header_ppa)) {
        if (new_version->write_ts < last_read_ts) {
            return -TX_ERROR_ABORT;
        }

        new_version->next = *header_ppa;
        set_version_index(header_ppa, new_version_idx);
        new_version->prev_field = header_ppa;
        return TX_STATUS_OK;
    }

    /* insert new version into time descending version list */
    ppa_ptr = header_ppa;
    version_ptr = get_version_data_from_ppa(ssd, ppa_ptr);
    while (new_version->write_ts < version_ptr->write_ts) {
        ppa_ptr = &version_ptr->next;
        if (map_entry_is_index(ppa_ptr)) {
            version_ptr = get_version_data_from_ppa(ssd, ppa_ptr);
        } else {
            version_ptr = NULL;
            break;
        }
    }

    /* handle tx repeat write one lpn */
    if (version_ptr != NULL) {
        if (new_version->write_ts == version_ptr->write_ts) {
            *repeat_version = version_ptr;
            return -TX_ERROR_DATA_REPEAT;
        }

        last_read_ts = version_ptr->read_ts;
    }

    if (new_version->write_ts < last_read_ts) {
        return -TX_ERROR_ABORT;
    }

    new_version->next = *ppa_ptr;
    set_version_index(ppa_ptr, new_version_idx);
    new_version->prev_field = ppa_ptr;

    if (version_ptr != NULL) {
        version_ptr->prev_field = &new_version->next;
    }
    
    return TX_STATUS_OK;
}

static size_t should_version_gc(struct ssd* ssd) {
    size_t used_count = MAX_LENGTH_OF_META_DATA_LIST - idx_pool_available_count(ssd->map_data_idx_pool);
    size_t threshold_count = (size_t)(MAX_LENGTH_OF_META_DATA_LIST * ssd->old_version_gc_threshold);

    if (used_count > threshold_count) {
        return used_count - threshold_count;
    } else {
        return 0;
    }
}

static inline bool is_garbage_version(struct ssd *ssd, struct map_data *map_info) {
    return map_info->write_ts < ssd->min_ts_active;
}

static size_t do_version_gc(struct ssd* ssd, size_t need_space) {
    size_t gc_num = 0;
    struct map_data *map_info;
    struct nand_page *page;

    while ((map_info = pqueue_peek(ssd->committed_queue)) != NULL) {
        if (map_info->status != TX_MAP_DATA_COMMITED) {
            pqueue_pop(ssd->committed_queue);
            continue;
        }

        if (!is_garbage_version(ssd, map_info)) {
            break;
        }

        pqueue_pop(ssd->committed_queue);
        ssd_delete_data_version(ssd, map_info);

        if (!map_entry_is_index(&map_info->next)) {
            /* update original PPN */
            if (mapped_ppa(&map_info->next)) {
                mark_page_invalid(ssd, &map_info->next);
                set_rmap_ent(ssd, INVALID_LPN, &map_info->next);
            }

            *(map_info->prev_field) = map_info->ppn;
            page = get_pg(ssd, &map_info->ppn);
            page->status = PG_VALID;
            set_rmap_ent(ssd, map_info->lpn, &map_info->ppn);
        } else {
            femu_log("gc version is not the tail of version list");
        }

        ssd_free_map_data_entry(ssd, map_info);
        ++gc_num;
        if (gc_num >= need_space) {
            break;
        }
    }

    return gc_num;
}

static int64_t ssd_twrite_one_lpn(struct ssd *ssd, uint32_t txid, uint64_t lpn, int64_t stime) {
    struct ppa new_ppa;
    tx_table_entry* tx_meta_data = &ssd->tx_table[txid];
    map_data *map_info = NULL, *repeat_version = NULL;
    int ret, alloc_idx;
    uint64_t curlat = 0;

    if (tx_meta_data->lpn_count >= MAX_LPN_PER_TX) {
        femu_log("too much lpn for one tx");
        return -TX_ERROR_NO_BUF;
    }

    /* get new phy page*/
    new_ppa = get_new_page(ssd);
    /* alloc a map data for lpn */
    alloc_idx = idx_pool_alloc(ssd->map_data_idx_pool);
    if (alloc_idx < 0) {
        femu_log("alloc map data entry failed");
        return -TX_ERROR_NO_BUF;
    }

    map_info = &ssd->map_data_table[alloc_idx];
    tx_meta_data->map_data_index_array[tx_meta_data->lpn_count] = alloc_idx;

    map_info->lpn = lpn;
    map_info->ppn = new_ppa;
    map_info->status = TX_MAP_DATA_UNCOMMITTED;
    map_info->write_ts = tx_meta_data->tx_timestamp;
    map_info->read_ts = 0;

    ret = ssd_insert_data_version(ssd, lpn, map_info, &repeat_version);
    if (ret == TX_STATUS_OK) {
        mark_page_uncommitted(ssd, &new_ppa, alloc_idx);
    } else if (ret == -TX_ERROR_ABORT) {
        /* Past writes overwrite future reads */
        map_info->status = TX_MAP_DATA_INVALID;
        idx_pool_free(ssd->map_data_idx_pool, alloc_idx);
        return -TX_ERROR_ABORT;
    } else if (ret == -TX_ERROR_DATA_REPEAT) {
        /* free the new allocated map data enrty and update info into old entry */
        map_info->status = TX_MAP_DATA_INVALID;
        idx_pool_free(ssd->map_data_idx_pool, alloc_idx);

        mark_page_invalid(ssd, &(repeat_version->ppn));
        repeat_version->ppn = new_ppa;
        mark_page_uncommitted(ssd, &new_ppa, (uint32_t)get_map_data_entry_index(ssd, repeat_version));
        tx_meta_data->map_data_index_array[tx_meta_data->lpn_count] = INVALID_MAP_DATA_INDEX;
    } else {
        femu_log("return invalid value");
        return -TX_ERROR_ABORT;
    }

    set_rmap_ent(ssd, lpn, &new_ppa);
    /* need to advance the write pointer here */
    tx_meta_data->lpn_count++;
    ssd_advance_write_pointer(ssd);

    struct nand_cmd swr;
    swr.type = USER_IO;
    swr.cmd = NAND_WRITE;
    swr.stime = stime;
    /* get latency statistics */
    curlat = ssd_advance_status(ssd, &new_ppa, &swr);

    return (int64_t)curlat;
}

static uint64_t ssd_twrite(struct ssd *ssd, NvmeRequest *req) {
    struct ssdparams *spp = &ssd->sp;
    NvmeTxWriteCmd* cmd = (NvmeTxWriteCmd*)&req->cmd;
    uint32_t txid = le32_to_cpu(cmd->txid);
    int len  = le16_to_cpu(cmd->nlb) + 1;
    uint64_t lba = le64_to_cpu(cmd->slba);
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    uint64_t lpn;
    int64_t ret;
    uint64_t curlat = 0, maxlat = 0;
    size_t need_space, gc_space;
    tx_table_entry *tx_meta_data;
    uint64_t min_active_ts;
    int r;

    femu_debug("twrite with txid: %u", (unsigned)txid);

    req->cqe.n.result = cpu_to_le32(TX_STATUS_OK);
    if (!(txid < MAX_TX_NUM && entry_in_use(&ssd->tx_table[txid]))) {
        femu_log("write command carry invalid txid");
        return 0;
    }

    tx_meta_data = &ssd->tx_table[txid];
    if (tx_meta_data->status != TX_INIT) {
        femu_log("target tx is already committed or abort");
        return 0;
    }

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
        return 0;
    }

    while (should_gc_high(ssd)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ssd, true);
        if (r == -1)
            break;
    }

    min_active_ts = le64_to_cpu(cmd->minTsActive);
    if (min_active_ts > ssd->min_ts_active) {
        ssd->min_ts_active = min_active_ts;
    }

    need_space = should_version_gc(ssd);
    if (need_space > 0) {
        gc_space = do_version_gc(ssd, need_space);
        if (gc_space < need_space) {
            femu_log("version gc free space lower");
        }
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ret = ssd_twrite_one_lpn(ssd, txid, lpn, req->stime);
        if (ret < 0) {
            __ssd_abort(ssd, txid);
            req->cqe.n.result = cpu_to_le32(TX_ERROR_ABORT);
            break;
        }
        
        curlat = (uint64_t)ret;
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

static uint64_t __ssd_commit(struct ssd *ssd, uint32_t txid) {
    tx_table_entry* tx_meta_data = NULL;
    map_data* map_info = NULL;
    uint32_t map_data_index;
    struct ppa ppn;
    struct nand_page* page;
    int ret;

    femu_debug("commit with txid: %u", (unsigned)txid);
    if (!(txid < MAX_TX_NUM && entry_in_use(&ssd->tx_table[txid]))) {
        femu_log("write command carry invalid txid");
        return 0;
    }

    /* tx with TX_COMMIT flag will redo when SPOR */
    tx_meta_data = &ssd->tx_table[txid];
    if (tx_meta_data->status != TX_INIT) {
        femu_log("tx is already commit or abort, status %d", tx_meta_data->status);
        return 0;
    }
    tx_meta_data->status = TX_COMMIT;

    for (int i = 0; i < tx_meta_data->lpn_count; i++) {
        map_data_index = tx_meta_data->map_data_index_array[i];
        if (map_data_index == INVALID_MAP_DATA_INDEX) {
            /* skip repeat LPN */
            continue;
        }

        map_info = &ssd->map_data_table[map_data_index];
        map_info->status = TX_MAP_DATA_COMMITED;
        pqueue_insert(ssd->committed_queue, map_info);
        
        /* page status uncommitted --> old version */
        ppn = map_info->ppn;
        page = get_pg(ssd, &ppn);
        page->status = PG_OLD_VERSION;
    }

    set_entry_unused(tx_meta_data);
    ret = idx_pool_free(ssd->tx_idx_pool, txid);
    if (ret < 0) {
        femu_log("free idx failed");
        return 0;
    }

    return 0;
}

static uint64_t ssd_commit(struct ssd *ssd, NvmeRequest *req) {
    NvmeTxWriteCmd* cmd = (NvmeTxWriteCmd*)&req->cmd;
    uint32_t txid = le32_to_cpu(cmd->txid);

    return __ssd_commit(ssd, txid);
}

static uint64_t __ssd_abort(struct ssd *ssd, uint32_t txid) {
    tx_table_entry* tx_meta_data = NULL;
    uint32_t map_data_index;
    struct map_data *map_info;
    struct ppa ppn;
    int ret;

    femu_debug("abort with txid: %u", (unsigned)txid);
    if (!(txid < MAX_TX_NUM && entry_in_use(&ssd->tx_table[txid]))) {
        femu_log("abort command carry invalid txid");
    }

    tx_meta_data = &ssd->tx_table[txid];
    if (tx_meta_data->status != TX_INIT) {
        femu_log("tx is already commit or abort, status %d", tx_meta_data->status);
        return 0;
    }
    tx_meta_data->status = TX_ABORT;

    for (int i = 0; i < tx_meta_data->lpn_count; i++) {
        map_data_index = tx_meta_data->map_data_index_array[i];
        if (map_data_index == INVALID_MAP_DATA_INDEX) {
            continue;
        }

        map_info = &ssd->map_data_table[map_data_index];
        ppn = map_info->ppn;

        /* delete version from list and mark page invalid */
        ssd_delete_data_version(ssd, map_info);
        mark_page_invalid(ssd, &ppn);
        set_rmap_ent(ssd, INVALID_LPN, &ppn);
    }

    set_entry_unused(tx_meta_data);
    ret = idx_pool_free(ssd->tx_idx_pool, txid);
    if (ret < 0) {
        femu_log("free idx failed");
        return 0;
    }

    return 0;
}

static uint64_t ssd_abort(struct ssd *ssd, NvmeRequest *req) {
    NvmeTxWriteCmd* cmd = (NvmeTxWriteCmd*)&req->cmd;
    uint32_t txid = le32_to_cpu(cmd->txid);

    return __ssd_abort(ssd, txid);
}

static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct ssd *ssd = n->ssd;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    size_t need_space;
    int i;

    while (!*(ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    /* FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully */
    ssd->to_ftl = n->to_ftl;
    ssd->to_poller = n->to_poller;
    ssd->check_tx_timeout_timer = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);

    while (1) {
        for (i = 1; i <= n->num_poller; i++) {
            if (!ssd->to_ftl[i] || !femu_ring_count(ssd->to_ftl[i]))
                continue;

            rc = femu_ring_dequeue(ssd->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            ftl_assert(req);
            switch (req->cmd.opcode) {
            case NVME_CMD_T_BEGIN:
                lat = ssd_begin(ssd, req);
                break;
            case NVME_CMD_T_ABORT:
                lat = ssd_abort(ssd, req);
                break;
            case NVME_CMD_T_WRITE:
                lat = ssd_twrite(ssd, req);
                break;
            case NVME_CMD_T_READ:
                lat = ssd_tread(ssd, req);
                break;
            case NVME_CMD_T_COMMIT:
                lat = ssd_commit(ssd, req);
                break;
            case NVME_CMD_WRITE:
                lat = ssd_write(ssd, req);
                break;
            case NVME_CMD_READ:
                lat = ssd_read(ssd, req);
                break;
            case NVME_CMD_DSM:
                lat = 0;
                break;
            default:
                //ftl_err("FTL received unkown request type, ERROR\n");
                ;
            }

            req->reqlat = lat;
            req->expire_time += lat;

            rc = femu_ring_enqueue(ssd->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            /* clean one line if needed (in the background) */
            if (should_gc(ssd)) {
                do_gc(ssd, false);
            }

            need_space = should_version_gc(ssd);
            if (need_space > 0) {
                do_version_gc(ssd, need_space);
            }
        }

        check_timeout_tx(ssd);
    }

    return NULL;
}

