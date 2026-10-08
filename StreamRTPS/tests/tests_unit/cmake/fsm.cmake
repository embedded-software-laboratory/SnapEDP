add_gossip_unit_test(test_fsm test_fsm.cpp streamrtps
    table_transition unknown_event_ignored event_order timeout_fires
    timeout_zero_waits timeout_rearmed_on_reentry timeout_cancelled_by_transition
    timeout_changed_in_observer post_from_action payload_delivered
    observer_called stop_while_blocked post_after_stop concurrent_post
    entry_after_action entry_on_self_loop internal_transition)
