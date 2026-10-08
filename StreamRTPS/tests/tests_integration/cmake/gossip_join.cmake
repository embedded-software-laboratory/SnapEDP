add_gossip_test(integration test_gossip_join test_gossip_join.cpp
    request_lost_once request_lost_until_bound skip_list_next_partner
    response_lost response_late_duplicate response_duplicated
    request_while_busy defer_cap_overflow defer_disabled
    server_policy_0 server_policy_2
    partner_dies_mid_handshake unresolvable_locator)
