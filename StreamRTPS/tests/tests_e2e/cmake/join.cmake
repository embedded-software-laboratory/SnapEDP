add_gossip_test(e2e test_e2e_join test_e2e_join.cpp
    early_join_8
    late_join_mid_8 late_join_high_8 late_join_low_8
    late_join_mid_32 late_join_high_32 late_join_low_32
    mass_late_join_16
    join_lossy_lossy-10 join_lossy_reorder
    join_during_partition join_while_root_dies joiner_with_many_endpoints)
