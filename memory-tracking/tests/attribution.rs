use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Barrier};
use std::task::{Context as TaskContext, Poll, Waker};

use delta_kernel_memory_tracking::{capture, AllocationSource, Charge, Context, TrackingAllocator};

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

#[test]
fn allocation_origin_survives_another_context_and_thread() {
    let origin = Context::new(1, None);
    let other = Context::new(2, None);
    let bytes = {
        let _scope = origin.enter();
        vec![42u8; 4096]
    };
    assert_eq!(origin.snapshot().total.live_bytes, 4096);
    std::thread::spawn(move || {
        let _scope = other.enter();
        drop(bytes);
        assert_eq!(other.snapshot().total.allocated_bytes, 0);
    })
    .join()
    .unwrap();
    assert_eq!(origin.snapshot().total.live_bytes, 0);
    assert_eq!(origin.snapshot().total.peak_bytes, 4096);
}

#[test]
fn sources_share_one_peak_and_hierarchy_is_inclusive() {
    let query = Context::new(1, None);
    let phase = Context::new(2, Some(query.clone()));
    let a = Charge::new(Some(phase.clone()), AllocationSource::Arrow, 100);
    drop(a);
    let b = Charge::new(Some(phase.clone()), AllocationSource::Abi, 200);
    assert_eq!(phase.snapshot().total.peak_bytes, 200);
    assert_eq!(query.snapshot().total.live_bytes, 200);
    assert_eq!(
        phase.snapshot().sources[AllocationSource::Arrow as usize].peak_bytes,
        100
    );
    drop(b);
    assert_eq!(query.snapshot().total.live_bytes, 0);
    assert_eq!(query.snapshot().total.allocated_bytes, 300);
}

#[test]
fn realloc_preserves_owner_data_alignment_and_zeroed_storage() {
    let origin = Context::new(1, None);
    let other = Context::new(2, None);
    for align in [1, 16, 4096] {
        let layout = Layout::from_size_align(19, align).unwrap();
        let pointer = {
            let _scope = origin.enter();
            unsafe { ALLOCATOR.alloc_zeroed(layout) }
        };
        assert!(!pointer.is_null());
        assert_eq!(pointer as usize % align, 0);
        assert!(unsafe { std::slice::from_raw_parts(pointer, 19) }
            .iter()
            .all(|b| *b == 0));
        unsafe { pointer.write(73) };
        let pointer = {
            let _scope = other.enter();
            unsafe { ALLOCATOR.realloc(pointer, layout, 71) }
        };
        assert!(!pointer.is_null());
        assert_eq!(unsafe { pointer.read() }, 73);
        assert_eq!(pointer as usize % align, 0);
        assert_eq!(origin.snapshot().total.live_bytes, 71);
        let layout = Layout::from_size_align(71, align).unwrap();
        let pointer = unsafe { ALLOCATOR.realloc(pointer, layout, 3) };
        assert!(!pointer.is_null());
        assert_eq!(origin.snapshot().total.live_bytes, 3);
        // An impossible extended layout fails without releasing the original allocation.
        assert!(unsafe {
            ALLOCATOR.realloc(
                pointer,
                Layout::from_size_align(3, align).unwrap(),
                usize::MAX,
            )
        }
        .is_null());
        assert_eq!(unsafe { pointer.read() }, 73);
        unsafe { ALLOCATOR.dealloc(pointer, Layout::from_size_align(3, align).unwrap()) };
        assert_eq!(origin.snapshot().total.live_bytes, 0);
        assert_eq!(other.snapshot().total.allocated_bytes, 0);
    }
    assert_eq!(origin.snapshot().total.tracking_bytes, 0);
}

#[test]
fn nested_and_out_of_order_scopes_remain_memory_safe() {
    let a = Context::new(1, None);
    let b = Context::new(2, None);
    let first = a.enter();
    let second = b.enter();
    drop(first);
    drop(second);
    // Out-of-order scope exit can restore an earlier context, but cannot leave a dangling pointer.
    assert_eq!(capture().unwrap().snapshot().id, 1);
    let _restore = delta_kernel_memory_tracking::Scope::enter(None);
}

#[test]
fn concurrent_external_sources_retain_exact_live_counts() {
    let context = Context::new(1, None);
    let barrier = Arc::new(Barrier::new(5));
    let threads: Vec<_> = [
        AllocationSource::Rust,
        AllocationSource::Arrow,
        AllocationSource::Abi,
        AllocationSource::Other,
    ]
    .into_iter()
    .map(|source| {
        let context = context.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let charge = Charge::new(Some(context), source, 1024);
            barrier.wait();
            barrier.wait();
            drop(charge);
        })
    })
    .collect();
    barrier.wait();
    assert_eq!(context.snapshot().total.live_bytes, 4096);
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(context.snapshot().total.live_bytes, 0);
    assert_eq!(context.snapshot().total.peak_bytes, 4096);
}

struct AllocateOnPollAndDrop;

impl Future for AllocateOnPollAndDrop {
    type Output = (u64, Vec<u8>);

    fn poll(self: std::pin::Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Ready((capture().unwrap().snapshot().id, vec![0u8; 100]))
    }
}

impl Drop for AllocateOnPollAndDrop {
    fn drop(&mut self) {
        let bytes = vec![0u8; 200];
        std::hint::black_box(bytes);
    }
}

#[test]
fn future_poll_and_cancellation_restore_context_on_each_thread() {
    let context = Context::new(7, None);
    let tracked = context.track(AllocateOnPollAndDrop);
    let bytes = std::thread::spawn(move || {
        let mut future = pin!(tracked);
        let Poll::Ready((id, bytes)) = future
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
        else {
            panic!()
        };
        assert_eq!(id, 7);
        assert!(capture().is_none());
        bytes
    })
    .join()
    .unwrap();
    assert_eq!(context.snapshot().total.live_bytes, 100);
    assert_eq!(context.snapshot().total.peak_bytes, 300);
    drop(bytes);
    assert_eq!(context.snapshot().total.live_bytes, 0);
    drop(context.track(AllocateOnPollAndDrop));
    assert_eq!(context.snapshot().total.allocated_bytes, 500);
}
