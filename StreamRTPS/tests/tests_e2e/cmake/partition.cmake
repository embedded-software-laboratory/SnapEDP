add_gossip_test(e2e test_e2e_partition test_e2e_partition.cpp
    merge_k_plus_k_2 merge_k_plus_k_4 merge_k_plus_k_16 merge_uneven
    three_way_split split_then_heal_before_expiry split_then_heal_after_expiry
    one_way_partition flapping root_isolated churn_during_partition)
