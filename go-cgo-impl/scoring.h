#ifndef SCORING_H
#define SCORING_H
#include <stdint.h>

// SIMD delta update for PPSMatrix: adds delta[o] (int32) to matrix[p*num_orders+o] (int16)
// for all PPS rows p and orders o. Saturates to [-32767, 32767].
void update_all_scores_i16(int16_t* __restrict__ score_matrix,
                           const int32_t* __restrict__ delta_scores,
                           int num_pps, int num_orders);

// Zero out score_matrix[p*num_orders + order_id] for all PPS rows p.
// Used during order eviction.
void evict_order_scores(int16_t* __restrict__ score_matrix,
                        int order_id, int num_pps, int num_orders);

// Initialise score_matrix[p*num_orders+o] = weights_i16[o] for all pairs p and orders o.
// Used when bringing up a new PPSMatrix row.
void init_scores_i16(int16_t* __restrict__ score_matrix,
                     const int16_t* __restrict__ weights,
                     int num_pps, int num_orders);

#endif
