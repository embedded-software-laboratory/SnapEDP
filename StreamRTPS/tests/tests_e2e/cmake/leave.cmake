add_gossip_test(e2e test_e2e_leave test_e2e_leave.cpp
    clean_non_root_8 clean_non_root_32 crash_non_root_8
    root_leaves_clean_8 root_leaves_crash_8 root_leaves_clean_32 root_leaves_crash_32
    mass_leave_16 leave_as_join_partner
    rejoin_same_prefix_before_expiry rejoin_same_prefix_after_expiry
    rejoin_new_prefix rolling_restart last_two)
if(TEST e2e_leave_as_join_partner)
    set_tests_properties(e2e_leave_as_join_partner
        PROPERTIES LABELS "e2e;knownbug")
endif()
