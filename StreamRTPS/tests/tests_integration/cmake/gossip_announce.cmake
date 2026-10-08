add_gossip_test(integration test_gossip_announce test_gossip_announce.cpp
    announce_lost_one_peer dispose_lost announce_dispose_reordered
    create_remove_create announcement_duplicated multi_frame_announce
    reannounce_policy_0 reannounce_policy_1 reannounce_policy_2
    hash_in_announcement matching_after_announce)
