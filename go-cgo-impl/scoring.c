#include "scoring.h"
#include <stdint.h>

// update_all_scores_i16: adds delta[o] (int32) to every PPS row of the int16 matrix.
// Uses saturating arithmetic: result clamped to [-32767, 32767].
// __restrict__ enables SIMD auto-vectorisation (NEON on ARM, AVX2 on x86).
void update_all_scores_i16(int16_t* __restrict__ score_matrix,
                            const int32_t* __restrict__ delta_scores,
                            int num_pps, int num_orders) {
    for (int p = 0; p < num_pps; p++) {
        int16_t* __restrict__ row = score_matrix + (long)p * num_orders;
        for (int o = 0; o < num_orders; o++) {
            int32_t v = (int32_t)row[o] + delta_scores[o];
            if (v > 32767)  v = 32767;
            if (v < -32767) v = -32767;
            row[o] = (int16_t)v;
        }
    }
}

// evict_order_scores: zero the score slot for order_id across all PPS rows.
void evict_order_scores(int16_t* __restrict__ score_matrix,
                        int order_id, int num_pps, int num_orders) {
    for (int p = 0; p < num_pps; p++) {
        score_matrix[(long)p * num_orders + order_id] = 0;
    }
}

// init_scores_i16: copy weights_i16[o] into every PPS row of the matrix.
void init_scores_i16(int16_t* __restrict__ score_matrix,
                     const int16_t* __restrict__ weights,
                     int num_pps, int num_orders) {
    for (int p = 0; p < num_pps; p++) {
        int16_t* __restrict__ row = score_matrix + (long)p * num_orders;
        for (int o = 0; o < num_orders; o++) {
            row[o] = weights[o];
        }
    }
}
