add_gossip_test(e2e test_e2e_cost test_e2e_cost.cpp
    quiescence_8 quiescence_32 resync_bounded
    message_budget_8 message_budget_32
    convergence_time_budget_8 convergence_time_budget_32)
foreach(t quiescence_8 quiescence_32 resync_bounded message_budget_8 message_budget_32
          convergence_time_budget_8 convergence_time_budget_32)
    if(TEST e2e_${t})
        set_tests_properties(e2e_${t} PROPERTIES LABELS "e2e;cost")
    endif()
endforeach()
