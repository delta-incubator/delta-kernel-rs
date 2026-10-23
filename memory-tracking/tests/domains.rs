use delta_kernel_memory_tracking::{
    add_external, domain_snapshot, release_external, without_tracking, AccountingDomain,
    AllocationSource, Context, TrackingAllocator,
};

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

#[test]
fn aggregate_domains_partition_origin_once_and_include_instrumentation() {
    let metadata = AllocationSource::Instrumentation as usize;
    let other = AllocationSource::Other as usize;
    let before = domain_snapshot(AccountingDomain::Untagged).sources[metadata].live_bytes;
    let query = Context::new(1, None);
    let phase = Context::new(2, Some(query.clone()));
    let records = domain_snapshot(AccountingDomain::Untagged).sources[metadata].live_bytes;
    assert!(records > before);
    {
        let _scope = phase.enter();
        let mut bytes = without_tracking(|| vec![1u8; 123]);
        assert_eq!(query.snapshot().total.live_bytes, 0);
        assert_eq!(
            domain_snapshot(AccountingDomain::Untagged).sources[metadata].live_bytes,
            records + 123
        );
        bytes.reserve_exact(456);
        let capacity = bytes.capacity() as u64;
        assert_eq!(query.snapshot().total.live_bytes, 0);
        assert_eq!(
            domain_snapshot(AccountingDomain::Untagged).sources[metadata].live_bytes,
            records + capacity
        );
        drop(bytes);
    }
    add_external(Some(&phase), AllocationSource::Other, 200);
    add_external(None, AllocationSource::Other, 300);
    assert_eq!(query.snapshot().sources[other].live_bytes, 200);
    assert_eq!(
        domain_snapshot(AccountingDomain::Tagged).sources[other].live_bytes,
        200
    );
    assert_eq!(
        domain_snapshot(AccountingDomain::Untagged).sources[other].live_bytes,
        300
    );
    assert_eq!(
        domain_snapshot(AccountingDomain::Process).sources[other].live_bytes,
        500
    );
    release_external(Some(&phase), AllocationSource::Other, 200);
    release_external(None, AllocationSource::Other, 300);
    drop(phase);
    drop(query);
    assert_eq!(
        domain_snapshot(AccountingDomain::Untagged).sources[metadata].live_bytes,
        before
    );
}
