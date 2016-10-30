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
    int res = fid->rs[FID_I2SBI(fid, i)] + fid->rb[FID_I2BI(fid, i)] + __builtin_popcount(FID_CHOP_BLOCK_I(fid, fid->bs[FID_I2BI(fid, i)], i));
    return b ? res : i - res;
}

int fid_select(fid *fid, int b, int i) {
    int l, r;
    l = FID_I2SBI(fid, i);
    r = FID_I2SBI(fid, fid->n) + 1;
    while (l + 1 < r) {
        int m = (l + r) >> 1;
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
        int m = (l + r) >> 1;
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
        int m = (l + r) >> 1;
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

    int32_t mid = ((long long)lower + upper) >> 1;
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

int32_t wt_access(const wt_tree *tree, int i) {
    wt_node *cur = tree->root;
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (lower < upper) {
        int32_t mid = ((long long)lower + upper) >> 1;
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
    return lower;
}

int wt_rank(const wt_tree *tree, int32_t value, int i) {
    wt_node *cur = tree->root;
    int32_t lower = MIN_ALPHABET, upper = MAX_ALPHABET;
    while (lower < upper) {
        int32_t mid = ((long long)lower + upper) >> 1;

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
        int32_t mid = ((long long)lower + upper) >> 1;

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
        int32_t mid = ((long long)lower + upper) >> 1;

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
