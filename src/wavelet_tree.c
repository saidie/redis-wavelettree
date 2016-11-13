#include "wavelet_tree.h"

/*
 * Fully Indexable Dictionary
 */

fid *fid_new(uint32_t *bytes, size_t n) {
    fid *fid = calloc(1, sizeof(*fid));
    fid->bs = bytes;
    fid->n = n;
    fid->rs = calloc(FID_I2SBI(fid, fid->n) + 1, sizeof(uint32_t));
    fid->rb = calloc(FID_I2BI(fid, fid->n) + 1, sizeof(uint16_t));

    int i, srank = 0, brank = 0;
    uint32_t *rs = fid->rs;
    uint16_t *rb = fid->rb;
    *(rs++) = 0;
    *(rb++) = 0;
    for(i = 1; i <= FID_I2BI(fid, fid->n); ++i) {
        int pc = __builtin_popcount(*(bytes++));
        srank += pc;
        brank += pc;

        if (!(i & FID_MASK_BSEP(fid))) {
            brank = 0;
            *(rs++) = srank;
        }
        *(rb++) = brank;
    }

    return fid;
}

void fid_free(fid *fid) {
    free(fid->bs);
    free(fid->rs);
    free(fid->rb);
    free(fid);
}

static inline int fid_rank(fid *fid, int b, size_t i) {
    if (fid->n < i) i = fid->n;
    int res = fid->rs[FID_I2SBI(fid, i)] + fid->rb[FID_I2BI(fid, i)] + __builtin_popcount(FID_CHOP_BLOCK_I(fid, fid->bs[FID_I2BI(fid, i)], i));
    return b ? res : i - res;
}

int fid_select(fid *fid, int b, int i) {
    int l, r;
    l = FID_I2SBI(fid, i);
    r = FID_I2SBI(fid, fid->n) + 1;
    while (l + 1 < r) {
        int m = MID(l, r);
        int rank = fid->rs[m];
        if (!b) rank = FID_SBI2I(fid, m) - rank;
        if (i <= rank)
            r = m;
        else
            l = m;
    }
    if (b)
        i -= fid->rs[l];
    else
        i -= FID_SBI2I(fid, l) - fid->rs[l];
    r = FID_SBI2BI(fid, l + 1);
    int offset = l = FID_SBI2BI(fid, l);
    if (FID_I2BI(fid, fid->n) + 1 < r)
        r = FID_I2BI(fid, fid->n) + 1;
    while (l + 1 < r) {
        int m = MID(l, r);
        int rank = fid->rb[m];
        if (!b) rank = FID_BI2I(fid, m - offset) - rank;
        if (i <= rank)
            r = m;
        else
            l = m;
    }
    if (b)
        i -= fid->rb[l];
    else
        i -= FID_BI2I(fid, l - offset) - fid->rb[l];
    unsigned int byte = fid->bs[l], res = FID_BI2I(fid, l);
    int mask = 0xFFFF0000;

    l = 0; r = 32;

    if (!b) byte = ~byte;
    while(l + 1 < r) {
        int m = MID(l, r);
        int rank = __builtin_popcount(byte & mask);
        if (i <= rank) {
            mask <<= (r - m) >> 1;
            r = m;
        }
        else {
            mask >>= (m - l) >> 1;
            l = m;
        }
    }

    return res + l;
}

/*
 * Wavelet Tree
 */

wt_node *wt_node_new(wt_node *parent) {
    wt_node *node = calloc(1, sizeof(wt_node));
    node->parent = parent;
    return node;
}

wt_tree *wt_new(void) {
    wt_tree *tree;
    tree = calloc(1, sizeof(*tree));
    tree->root = wt_node_new(NULL);
    return tree;
}

void _wt_build(wt_node *cur, int32_t *data, int n, int32_t lower, int32_t upper) {
    cur->n = n;

    if(lower == upper) return;

    int32_t mid = MID(lower, upper);
    uint32_t *bytes = calloc(FID_I2BI(fid, n) + ((n & FID_MASK_BI(fid)) ? 1 : 0), sizeof(uint32_t));

    int i, nl = 0;
    uint32_t *bhead = bytes;
    for(i = 0; i < n; ) {
        *bhead <<= 1;
        if (data[i] <= mid) {
            nl++;
        }
        else
            *bhead |= 1;
        ++i;
        if (!(i & FID_MASK_BI(fid)))
            ++bhead;
    }
    if (i & FID_MASK_BI(fid))
        *bhead <<= FID_NBIT_B(fid) - (i & FID_MASK_BI(fid));

    cur->fid = fid_new(bytes, n);

    int j, carry, tmp;
    for(i = 0; i < n; ++i) {
        carry = data[i];
        if (carry <= mid) {
            j = fid_rank(cur->fid, 0, i);
            carry += mid - lower + 1;
            while (i != j) {
                tmp = data[j];
                data[j] = carry;
                carry = tmp;

                if (carry <= mid) {
                    carry += mid - lower + 1;
                    j = fid_rank(cur->fid, 0, j);
                }
                else {
                    j = fid_rank(cur->fid, 1, j) + nl;
                }
            }
            data[i] = carry;
        }
    }
    for(i = 0; i < nl; ++i)
        data[i] -= mid - lower + 1;

    if (nl) {
        cur->left = wt_node_new(cur);
        _wt_build(cur->left, data, nl, lower, mid);
    }

    if (n - nl) {
        cur->right = wt_node_new(cur);
        _wt_build(cur->right, data + nl, n - nl, mid+1, upper);
    }

    if (DESTRUCTIVE_BUILD) return;

    for (i = 0; i < nl; ++i)
        data[i] += mid - lower + 1;

    for (i = 0; i < n; ++i) {
        carry = data[i];
        if (mid < carry) {
            if (i < nl)
                j = fid_select(cur->fid, 1, i + 1);
            else
                j = fid_select(cur->fid, 0, i - nl + 1);

            while (i != j) {
                tmp = data[j];
                data[j] = carry - (mid - lower + 1);
                carry = tmp;

                if (j < nl)
                    j = fid_select(cur->fid, 1, j + 1);
                else
                    j = fid_select(cur->fid, 0, j - nl + 1);
            }
            data[i] = carry - (mid - lower + 1);
        }
    }

    for(i = 0; i < n - nl; ++i)
        data[fid_select(cur->fid, 0, i+1)] += mid - lower + 1;
}

void wt_build(wt_tree *tree, int32_t *data, size_t len) {
    tree->len = len;

    _wt_build(tree->root, data, len, MIN_ALPHABET, MAX_ALPHABET);
}

void wt_node_free(wt_node *cur) {
    if (cur->left) wt_node_free(cur->left);
    if (cur->right) wt_node_free(cur->right);
    if (cur->fid) fid_free(cur->fid);
    free(cur);
}

void wt_free(wt_tree *tree) {
    wt_node_free(tree->root);
    free(tree);
}

int wt_access(const wt_tree *tree, size_t i, int32_t *res) {
    wt_node *cur = tree->root;
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (cur && lower < upper) {
        int32_t mid = MID(lower, upper);
        if (fid_rank(cur->fid, 0, i+1) - fid_rank(cur->fid, 0, i)) {
            i = fid_rank(cur->fid, 0, i);
            upper = mid;
            cur = cur->left;
        }
        else {
            i = fid_rank(cur->fid, 1, i);
            lower = mid + 1;
            cur = cur->right;
        }
    }
    *res = lower;
    return cur && lower == upper;
}

int wt_rank(const wt_tree *tree, int32_t value, int i) {
    wt_node *cur = tree->root;
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (lower < upper) {
        int32_t mid = MID(lower, upper);

        if (value <= mid) {
            if (!cur->left) return 0;
            i = fid_rank(cur->fid, 0, i);
            upper = mid;
            cur = cur->left;
        }
        else {
            if (!cur->right) return 0;
            i = fid_rank(cur->fid, 1, i);
            lower = mid + 1;
            cur = cur->right;
        }
    }
    return i;
}


int wt_select(const wt_tree *tree, int32_t v, int i) {
    wt_node *cur = tree->root;
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (lower < upper) {
        int32_t mid = MID(lower, upper);

        if (v <= mid) {
            if (!cur->left) return -1;
            cur = cur->left;
            upper = mid;
        }
        else {
            if (!cur->right) return -1;
            cur = cur->right;
            lower = mid + 1;
        }
    }

    if (cur->n < i) return -1;

    --i;
    while (cur->parent) {
        int left = cur == cur->parent->left;
        cur = cur->parent;
        i = fid_select(cur->fid, left ? 0 : 1, i + 1);
    }

    return i;
}

int wt_quantile(const wt_tree *tree, int k, int i, int j) {
    wt_node *cur = tree->root;
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (lower < upper) {
        int32_t mid = MID(lower, upper);

        int ln = fid_rank(cur->fid, 0, j) - fid_rank(cur->fid, 0, i);
        if (k <= ln) {
            i = fid_rank(cur->fid, 0, i);
            j = fid_rank(cur->fid, 0, j);
            upper = mid;
            cur = cur->left;
        }
        else {
            k -= ln;
            i = fid_rank(cur->fid, 1, i);
            j = fid_rank(cur->fid, 1, j);
            lower = mid + 1;
            cur = cur->right;
        }
    }
    return lower;
}

#define RANGE_FLAG_LEFT  0x1
#define RANGE_FLAG_RIGHT 0x2
#define RANGE_FLAG_BOTH (RANGE_FLAG_LEFT|RANGE_FLAG_RIGHT)

static inline const wt_node *_wt_range_branch(wt_node *cur, int *i, int *j, int32_t x, int32_t y, int32_t *lower, int32_t *upper) {
    int32_t mid;
    while (cur && *lower < *upper) {
        mid = MID(*lower, *upper);
        if (y <= mid) {
            *i = fid_rank(cur->fid, 0, *i);
            *j = fid_rank(cur->fid, 0, *j);
            *upper = mid;
            cur = cur->left;
        }
        else if (mid < x) {
            *i = fid_rank(cur->fid, 1, *i);
            *j = fid_rank(cur->fid, 1, *j);
            *lower = mid + 1;
            cur = cur->right;
        }
        else
            break;
    }
    return cur;
}

int _wt_range_freq_half(const wt_node *cur, int i, int j, int32_t boundary, int flags, int32_t lower, int32_t upper) {
    int freq = 0;
    int32_t mid;
    while (cur && lower < upper) {
        mid = MID(lower, upper);
        if (boundary <= mid) {
            if ((flags & RANGE_FLAG_RIGHT) && cur->right)
                freq += fid_rank(cur->fid, 1, j) - fid_rank(cur->fid, 1, i);
            i = fid_rank(cur->fid, 0, i);
            j = fid_rank(cur->fid, 0, j);
            upper = mid;
            cur = cur->left;
        }
        else {
            if ((flags & RANGE_FLAG_LEFT) && cur->left)
                freq += fid_rank(cur->fid, 0, j) - fid_rank(cur->fid, 0, i);
            i = fid_rank(cur->fid, 1, i);
            j = fid_rank(cur->fid, 1, j);
            lower = mid + 1;
            cur = cur->right;
        }
    }
    if (cur) freq += j - i;
    return freq;
}

int wt_range_freq(const wt_tree *tree, int i, int j, int32_t x, int32_t y) {
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    const wt_node *cur = _wt_range_branch(tree->root, &i, &j, x, y, &lower, &upper);
    if (!cur) return 0;
    if (lower == upper) return j - i;

    return _wt_range_freq_half(cur->left, fid_rank(cur->fid, 0, i), fid_rank(cur->fid, 0, j), x, RANGE_FLAG_RIGHT, lower, MID(lower, upper)) +
        _wt_range_freq_half(cur->right, fid_rank(cur->fid, 1, i), fid_rank(cur->fid, 1, j), y, RANGE_FLAG_LEFT, MID(lower, upper) + 1, upper);
}

int _wt_range_list_half(const wt_node *cur, int i, int j, int32_t boundary, int flags, int32_t lower, int32_t upper,
    void (*callback)(void*, int32_t, int), void *user_data) {
    int32_t mid, len = 0;
    while (cur && lower < upper) {
        mid = MID(lower, upper);
        if (boundary <= mid) {
            if ((flags & RANGE_FLAG_RIGHT) && cur->right)
                len += _wt_range_list_half(cur->right, fid_rank(cur->fid, 1, i), fid_rank(cur->fid, 1, j), boundary, RANGE_FLAG_BOTH, mid+1, upper, callback, user_data);
            i = fid_rank(cur->fid, 0, i);
            j = fid_rank(cur->fid, 0, j);
            upper = mid;
            cur = cur->left;
        }
        else {
            if ((flags & RANGE_FLAG_LEFT) && cur->left)
                len += _wt_range_list_half(cur->left, fid_rank(cur->fid, 0, i), fid_rank(cur->fid, 0, j), boundary, RANGE_FLAG_BOTH, lower, mid, callback, user_data);
            i = fid_rank(cur->fid, 1, i);
            j = fid_rank(cur->fid, 1, j);
            lower = mid + 1;
            cur = cur->right;
        }
    }
    if (cur && i < j) {
        callback(user_data, lower, j - i);
        ++len;
    }
    return len;
}

int wt_range_list(const wt_tree *tree, int i, int j, int32_t x, int32_t y, void (*callback)(void*, int32_t, int), void *user_data) {
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    const wt_node *cur = _wt_range_branch(tree->root, &i, &j, x, y, &lower, &upper);
    if (!cur) return 0;
    if (lower == upper) {
        if (i < j) {
            callback(user_data, lower, j - i);
            return 1;
        }
        return 0;
    }

    return _wt_range_list_half(cur->left, fid_rank(cur->fid, 0, i), fid_rank(cur->fid, 0, j), x, RANGE_FLAG_RIGHT, lower, MID(lower, upper), callback, user_data) +
        _wt_range_list_half(cur->right, fid_rank(cur->fid, 1, i), fid_rank(cur->fid, 1, j), y, RANGE_FLAG_LEFT, MID(lower, upper) + 1, upper, callback, user_data);
}

int32_t wt_prev_value(const wt_tree *tree, int i, int j, int32_t x, int32_t y) {
    y -= 1;
    const wt_node *cur = tree->root, *last_left_node = NULL;
    int last_left_i, last_left_j;
    int32_t mid, last_left_lower, last_left_upper, lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (cur && lower < upper) {
        mid = MID(lower, upper);
        if (y <= mid) {
            i = fid_rank(cur->fid, 0, i);
            j = fid_rank(cur->fid, 0, j);
            upper = mid;
            cur = cur->left;
        }
        else {
            if (x <= mid && cur->left && fid_rank(cur->fid, 0, i) < fid_rank(cur->fid, 0, j)) {
                last_left_node = cur->left;
                last_left_lower = lower;
                last_left_upper = mid;
                last_left_i = fid_rank(cur->fid, 0, i);
                last_left_j = fid_rank(cur->fid, 0, j);
            }
            i = fid_rank(cur->fid, 1, i);
            j = fid_rank(cur->fid, 1, j);
            lower = mid+1;
            cur = cur->right;
        }
    }
    if (cur && i < j) return lower;

    if (last_left_node) {
        i = last_left_i;
        j = last_left_j;
        lower = last_left_lower;
        upper = last_left_upper;
        cur = last_left_node;
        while (lower < upper) {
            mid = MID(lower, upper);
            if (cur->right && fid_rank(cur->fid, 1, i) < fid_rank(cur->fid, 1, j)) {
                i = fid_rank(cur->fid, 1, i);
                j = fid_rank(cur->fid, 1, j);
                lower = mid + 1;
                cur = cur->right;
            }
            else {
                i = fid_rank(cur->fid, 0, i);
                j = fid_rank(cur->fid, 0, j);
                upper = mid;
                cur = cur->left;
            }
        }
        if (i < j) return lower;
    }
    return y + 1;
}

int32_t wt_next_value(const wt_tree *tree, int i, int j, int32_t x, int32_t y) {
    x += 1;
    const wt_node *cur = tree->root, *last_right_node = NULL;
    int last_right_i, last_right_j;
    int32_t mid, last_right_lower, last_right_upper, lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (cur && lower < upper) {
        mid = MID(lower, upper);
        if (mid < x) {
            i = fid_rank(cur->fid, 1, i);
            j = fid_rank(cur->fid, 1, j);
            lower = mid + 1;
            cur = cur->right;
        }
        else {
            if (mid < y && cur->right && fid_rank(cur->fid, 1, i) < fid_rank(cur->fid, 1, j)) {
                last_right_node = cur->right;
                last_right_lower = mid + 1;
                last_right_upper = upper;
                last_right_i = fid_rank(cur->fid, 1, i);
                last_right_j = fid_rank(cur->fid, 1, j);
            }
            i = fid_rank(cur->fid, 0, i);
            j = fid_rank(cur->fid, 0, j);
            upper = mid;
            cur = cur->left;
        }
    }
    if (cur && i < j) return lower;

    if (last_right_node) {
        i = last_right_i;
        j = last_right_j;
        lower = last_right_lower;
        upper = last_right_upper;
        cur = last_right_node;
        while (lower < upper) {
            mid = MID(lower, upper);
            if (cur->left && fid_rank(cur->fid, 0, i) < fid_rank(cur->fid, 0, j)) {
                i = fid_rank(cur->fid, 0, i);
                j = fid_rank(cur->fid, 0, j);
                upper = mid;
                cur = cur->left;
            }
            else {
                i = fid_rank(cur->fid, 1, i);
                j = fid_rank(cur->fid, 1, j);
                lower = mid + 1;
                cur = cur->right;
            }
        }
        if (i < j) return lower;
    }
    return x - 1;
}

// Priority queue element for wt_topk
typedef struct topk_qe {
    const wt_node *node;
    int i, j;
    int32_t lower, upper;
} topk_qe;

topk_qe *topk_qe_new(const wt_node *node, int i, int j, int32_t lower, int32_t upper) {
    topk_qe *qe = malloc(sizeof(*qe));
    qe->node = node;
    qe->i = i;
    qe->j = j;
    qe->lower = lower;
    qe->upper = upper;
    return qe;
}

void topk_qe_free(topk_qe *qe) {
    free(qe);
}

int wt_topk(const wt_tree *tree, int i, int j, int k, void (*callback)(void*, int32_t, int), void *user_data) {
    heap *q = heap_new();
    heap_push(q, j - i, topk_qe_new(tree->root, i, j, MIN_ALPHABET, MAX_ALPHABET));

    int score, count = 0, ni, nj;
    topk_qe *qe;
    int32_t mid;
    while (count < k && heap_len(q) > 0) {
        heap_pop(q, &score, (void**)&qe);

        if (qe->lower == qe->upper) {
            ++count;
            callback(user_data, qe->lower, qe->j - qe->i);
        }
        else {
            mid = MID(qe->lower, qe->upper);

            // left
            ni = fid_rank(qe->node->fid, 0, qe->i);
            nj = fid_rank(qe->node->fid, 0, qe->j);
            if (ni < nj)
                heap_push(q, nj - ni, topk_qe_new(qe->node->left, ni, nj, qe->lower, mid));

            // right
            ni = fid_rank(qe->node->fid, 1, qe->i);
            nj = fid_rank(qe->node->fid, 1, qe->j);
            if (ni < nj)
                heap_push(q, nj - ni, topk_qe_new(qe->node->right, ni, nj, mid+1, qe->upper));
        }

        topk_qe_free(qe);
    }
    heap_free(q, (void (*)(void*))topk_qe_free);

    return count;
}

#define WT_RANGE_SORT_MIN 0
#define WT_RANGE_SORT_MAX 1

int _wt_range_sort(const wt_node *node, int i, int j, int k, int32_t lower, int32_t upper, int flags,
    void (*callback)(void*, int32_t, int), void *user_data) {
    if (lower == upper) {
        callback(user_data, lower, j - i);
        return k - 1;
    }

    int li, lj, ri, rj;
    int32_t mid;
    li = fid_rank(node->fid, 0, i);
    lj = fid_rank(node->fid, 0, j);
    ri = fid_rank(node->fid, 1, i);
    rj = fid_rank(node->fid, 1, j);
    mid = MID(lower, upper);

    if (flags == WT_RANGE_SORT_MIN) {
        if (li < lj)
            k = _wt_range_sort(node->left, li, lj, k, lower, mid, flags, callback, user_data);
    }
    else {
        if (ri < rj)
            k = _wt_range_sort(node->right, ri, rj, k, mid+1, upper, flags, callback, user_data);
    }

    if(k == 0) return k;

    if (flags == WT_RANGE_SORT_MIN) {
        if (ri < rj)
            k = _wt_range_sort(node->right, ri, rj, k, mid+1, upper, flags, callback, user_data);
    }
    else {
        if (li < lj)
            k = _wt_range_sort(node->left, li, lj, k, lower, mid, flags, callback, user_data);
    }
    return k;
}

int wt_range_mink(const wt_tree *tree, int i, int j, int k, void (*callback)(void*, int32_t, int), void *user_data) {
    return k - _wt_range_sort(tree->root, i, j, k, MIN_ALPHABET, MAX_ALPHABET, WT_RANGE_SORT_MIN, callback, user_data);
}

int wt_range_maxk(const wt_tree *tree, int i, int j, int k, void (*callback)(void*, int32_t, int), void *user_data) {
    return k - _wt_range_sort(tree->root, i, j, k, MIN_ALPHABET, MAX_ALPHABET, WT_RANGE_SORT_MAX, callback, user_data);
}
